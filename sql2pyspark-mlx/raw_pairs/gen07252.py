from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city")
w4 = Window.partitionBy(F.year("opened_date"), "city")
w5 = Window.partitionBy("city").orderBy(F.col("running_count_sqft").desc_nulls_last())
w6 = Window.partitionBy("city").orderBy("running_sum_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("running_max_sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w9 = Window.partitionBy("city").orderBy("running_count_sqft_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy(F.col("running_count_sqft_cume_dist").desc(), "id")

latest = (
    df_stores
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "city", "opened_date", "sqft")
)

enriched = (
    latest
    .select(
        "id",
        "city",
        "opened_date",
        F.max("sqft").over(w2).alias("running_max_sqft"),
        F.sum("sqft").over(w2).alias("running_sum_sqft"),
        F.count("sqft").over(w2).alias("running_count_sqft"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.round((F.col("running_max_sqft") / F.sum("running_max_sqft").over(w3)), 4).alias("running_max_sqft_share"),
        F.sum("running_max_sqft").over(w4).alias("opened_date_year_sum_running_max_sqft"),
        F.cume_dist().over(w5).alias("running_count_sqft_cume_dist"),
        F.sum("running_sum_sqft").over(w6).alias("cumulative_running_sum_sqft"),
        F.last("running_max_sqft", ignorenulls=True).over(w7).alias("last_known_running_max_sqft"),
    )
)

with_last = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.city",
        "s.opened_date",
        "s.running_max_sqft",
        "s.running_sum_sqft",
        "s.running_count_sqft",
        "s.running_count_sqft_cume_dist",
        "s.cumulative_running_sum_sqft",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

scored_stores = (
    with_last
    .filter(F.col("running_sum_sqft").isNotNull())
    .select(
        "id",
        "running_sum_sqft",
        "running_count_sqft",
        "cumulative_running_sum_sqft",
        "inventory_quantity",
        F.min("running_max_sqft").over(w9).alias("running_min_running_max_sqft"),
        F.sum("running_max_sqft").over(w9).alias("running_sum_running_max_sqft"),
        F.count("running_max_sqft").over(w9).alias("running_count_running_max_sqft"),
        F.max("running_max_sqft").over(w9).alias("running_max_running_max_sqft"),
        F.row_number().over(w10).alias("event_seq"),
    )
)

result = (
    scored_stores
    .withColumn("running_min_running_max_sqft_tier", F.when(F.col("running_min_running_max_sqft") >= 17100, "HIGH").otherwise("LOW"))
    .withColumn("running_max_running_max_sqft_tier", F.when(F.col("running_max_running_max_sqft") >= 14100, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["running_min_running_max_sqft_tier", "running_max_running_max_sqft_tier"], ["running_min_running_max_sqft_tier"], []],
        "running_min_running_max_sqft_tier",
        "running_max_running_max_sqft_tier",
    )
    .agg(
        F.percentile_approx("running_min_running_max_sqft", 0.95).alias("running_min_running_max_sqft_p95"),
        F.percentile_approx("inventory_quantity", [0.25, 0.5, 0.75]).alias("inventory_quantity_quartiles"),
        F.max("running_count_sqft").alias("max_running_count_sqft"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.avg("event_seq").alias("avg_event_seq"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("running_max_running_max_sqft_tier").desc())
)
