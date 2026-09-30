from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("days_since_last_restock", "id")
w2 = Window.orderBy("days_since_last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("store_id").orderBy(F.col("last_known_days_since_last_restock").desc_nulls_first(), "id")
w5 = Window.partitionBy("store_id").orderBy("last_known_days_since_last_restock", "id").rowsBetween(-5, -1)
w_part = Window.partitionBy("store_id").orderBy(F.col("store_id_max_days_since_last_restock").desc_nulls_first())
w7 = Window.orderBy("last_known_days_since_last_restock", "id")
w8 = Window.partitionBy("store_id").orderBy(F.col("store_id_min_store_id_max_days_since_last_restock").desc(), "id")

valid = (
    df_inventory
    .filter(F.col("product_id").isin([5, 10]))
    .select(
        "id",
        "store_id",
        "last_restock",
        F.col("quantity").cast("string").alias("quantity_str"),
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .select("src.*", F.col("s.store_name"))
)

metrics = (
    with_dims
    .withColumn("days_since_last_restock_bucket", F.ntile(4).over(w))
    .withColumn("last_known_days_since_last_restock", F.last("days_since_last_restock", ignorenulls=True).over(w2))
    .withColumn("store_id_max_days_since_last_restock", F.max("days_since_last_restock").over(w3))
)

latest = (
    metrics
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.opened_date"))
)

enriched_inventory = (
    enriched
    .select(
        "*",
        F.avg("last_known_days_since_last_restock").over(w5).alias("last_known_days_since_last_restock_avg_5_prior"),
        F.percent_rank().over(w_part).alias("store_id_max_days_since_last_restock_percent_rank"),
        F.min("store_id_max_days_since_last_restock").over(w3).alias("store_id_min_store_id_max_days_since_last_restock"),
        F.ntile(5).over(w7).alias("last_known_days_since_last_restock_bucket"),
    )
)

deduped = (
    enriched_inventory
    .withColumn("rank_in_group_2", F.row_number().over(w8))
    .filter(F.col("rank_in_group_2") == 1)
)

totals = (
    deduped
    .withColumn("quarter_start", F.date_trunc("QUARTER", "last_restock"))
    .withColumn("store_id_max_days_since_last_restock_percent_rank_tier", F.when(F.col("store_id_max_days_since_last_restock_percent_rank") >= 0.85, "HIGH").otherwise("LOW"))
    .withColumn("days_since_last_restock_tier", F.when(F.col("days_since_last_restock") >= 62, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["quarter_start", "store_id_max_days_since_last_restock_percent_rank_tier", "days_since_last_restock_tier"], ["quarter_start", "store_id_max_days_since_last_restock_percent_rank_tier"], ["quarter_start"], []],
        "quarter_start",
        "store_id_max_days_since_last_restock_percent_rank_tier",
        "days_since_last_restock_tier",
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.countDistinct("store_id").alias("distinct_store_ids_2"),
        F.round(F.avg("store_id_max_days_since_last_restock"), 2).alias("avg_store_id_max_days_since_last_restock"),
        F.percentile_approx("store_id_min_store_id_max_days_since_last_restock", [0.25, 0.5, 0.75]).alias("store_id_min_store_id_max_days_since_last_restock_quartiles"),
        F.percentile_approx("store_id_min_store_id_max_days_since_last_restock", 0.9).alias("store_id_min_store_id_max_days_since_last_restock_p90"),
        F.count("*").alias("event_count"),
        F.grouping_id().alias("grouping_id"),
    )
)

result = (
    totals
    .select(
        "distinct_store_ids",
        "distinct_store_ids_2",
        "avg_store_id_max_days_since_last_restock",
        "store_id_min_store_id_max_days_since_last_restock_quartiles",
        "store_id_min_store_id_max_days_since_last_restock_p90",
        "event_count",
    )
    .orderBy(
        F.col("store_id_min_store_id_max_days_since_last_restock_p90").asc_nulls_last(),
    )
)
