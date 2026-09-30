from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w_hist = Window.partitionBy("city").orderBy(F.col("sqft").desc())
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy(F.col("region_min_sqft").desc(), "id")
w5 = Window.partitionBy("city").orderBy("region_min_sqft", "id")
w6 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy(F.col("sqft_cume_dist").desc())
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

not_flagged = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") <= F.col("s.opened_date")), "left_anti")
)

scored = (
    not_flagged
    .withColumn("region_min_sqft", F.min("sqft").over(w))
    .withColumn("sqft_cume_dist", F.cume_dist().over(w_hist))
    .withColumn("running_sqft", F.sum("sqft").over(w3))
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w4))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

metrics = (
    ranked
    .select(
        "*",
        F.lead("sqft", 1).over(w5).alias("next_sqft"),
        F.sum("region_min_sqft").over(w6).alias("cumulative_region_min_sqft"),
        F.rank().over(w7).alias("sqft_cume_dist_rank"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("order_amount"), F.col("c.discount"))
)

grouped = (
    with_last
    .groupBy("region")
    .agg(
        F.percentile_approx("sqft_cume_dist_rank", 0.5).alias("sqft_cume_dist_rank_p50"),
        F.sum("sqft").alias("total_sqft"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.round(F.sum("sqft_cume_dist") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("sqft_cume_dist_per_london"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("row_count"),
    )
)

result = (
    grouped
    .select(
        "region",
        "total_sqft",
        "num_regions",
        "sqft_cume_dist_per_london",
        "num_rows",
        "row_count",
    )
    .orderBy(F.col("num_rows").desc())
)
