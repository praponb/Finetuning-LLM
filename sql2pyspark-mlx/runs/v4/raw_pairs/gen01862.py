from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region").orderBy(F.col("city_avg_sqft").desc_nulls_first())
w4 = Window.partitionBy("region").orderBy(F.col("running_sqft").asc_nulls_last())
w5 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("city_avg_sqft", "id").rowsBetween(-2, Window.currentRow)
w7 = Window.partitionBy("region").orderBy("sqft", "id")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

filtered = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-04-30").cast("date"))
    .select(
        "*",
        F.trim("store_name").alias("clean_store_name"),
        F.coalesce("region", F.lit("N/A")).alias("region_clean"),
    )
)

cleaned = (
    filtered
    .filter(F.col("opened_date") > F.lit("2023-12-02").cast("date"))
    .select(
        "*",
        F.concat_ws("-", "store_name", "region").alias("store_name_region_key"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.avg("sqft").over(w).alias("city_avg_sqft"),
        F.sum("sqft").over(w2).alias("running_sqft"),
    )
)

latest = (
    metrics
    .withColumn("dedup_rank", F.rank().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    latest
    .select(
        "*",
        F.cume_dist().over(w4).alias("running_sqft_cume_dist"),
        F.sum("running_sqft").over(w5).alias("cumulative_running_sqft"),
        F.avg("city_avg_sqft").over(w6).alias("city_avg_sqft_avg_2"),
        F.lag("sqft", 2).over(w7).alias("prev_sqft"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("order_status"), F.col("c.discount"))
)

matched_keys = (
    with_latest
    .select("id")
    .intersectAll(
        df_stores
        .filter(F.col("city") == "Singapore")
        .select("id")
    )
)

kept_rows = (
    with_latest
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

totals = (
    kept_rows
    .cube(
        F.when(F.col("discount") >= 0.36, "HIGH").otherwise("LOW").alias("discount_tier"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.percentile_approx("cumulative_running_sqft", [0.25, 0.5, 0.75]).alias("cumulative_running_sqft_quartiles"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
    )
    .filter(F.col("tokyo_count") > 1)
)

result = totals.select("discount_tier", "cumulative_running_sqft_quartiles", "tokyo_count")
