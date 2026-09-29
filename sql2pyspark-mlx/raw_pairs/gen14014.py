from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city", "region").orderBy(F.col("inventory_last_restock").desc_nulls_last(), "id")
w_running = Window.partitionBy("city", "region").orderBy("rn", "id")
w4 = Window.partitionBy(F.year("inventory_last_restock"), "city").orderBy("inventory_quantity_rounded", "id")
w5 = Window.partitionBy(F.date_trunc("week", "opened_date")).orderBy("opened_date", "id")
w6 = Window.orderBy("event_seq", "id")

filtered = (
    df_stores
    .filter((F.col("region").eqNullSafe(F.lit("Central"))) & F.col("city").isin(["Bangkok", "Singapore"]))
    .withColumn("store_name_slug", F.regexp_replace("store_name", " ", "-"))
    .withColumn("is_west", F.when(F.col("region") == "West", 1).otherwise(0))
)

latest_match = (
    filtered
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_name",
        "s.city",
        "s.region",
        "s.opened_date",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

ranked = (
    latest_match
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

valid = (
    ranked
    .filter(F.col("region").isNotNull() & ~F.col("region").isin(["West", "East", "Central"]))
    .select(
        "*",
        F.round("inventory_quantity", 0).alias("inventory_quantity_rounded"),
        F.coalesce("region", F.lit("NONE")).alias("region_clean"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "opened_date",
        "inventory_quantity",
        "rn",
        F.lag("inventory_quantity").over(w_running).alias("prev_inventory_quantity"),
        F.lag("inventory_quantity_rounded").over(w_running).alias("prev_inventory_quantity_rounded"),
        F.row_number().over(w_running).alias("event_seq"),
        F.row_number().over(w4).alias("event_seq_2"),
    )
)

enriched = (
    with_windows
    .withColumn("prev_event_seq_2", F.lag("event_seq_2", 1).over(w5))
    .withColumn("event_seq_bucket", F.ntile(3).over(w6))
)

latest_match_stores = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn_2", F.row_number().over(w))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.opened_date",
        "s.inventory_quantity",
        "s.rn",
        "s.prev_inventory_quantity_rounded",
        "s.event_seq_bucket",
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

result = (
    latest_match_stores
    .withColumn("activity_month", F.date_trunc("MONTH", "inventory_last_restock"))
    .withColumn("event_seq_bucket_tier", F.when(F.col("event_seq_bucket") >= 8, "LARGE").otherwise("SMALL"))
    .cube("activity_month", "event_seq_bucket_tier")
    .agg(
        F.percentile_approx("prev_inventory_quantity_rounded", [0.25, 0.5, 0.75]).alias("prev_inventory_quantity_rounded_quartiles"),
        F.round(F.avg("inventory_quantity"), 2).alias("avg_inventory_quantity"),
        F.max("prev_inventory_quantity_rounded").alias("max_prev_inventory_quantity_rounded"),
        F.percentile_approx("inventory_quantity", 0.95).alias("inventory_quantity_p95"),
        F.count("*").alias("event_count"),
        F.grouping("activity_month").alias("grouping_level"),
    )
    .orderBy(F.col("max_prev_inventory_quantity_rounded").desc())
)
