from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("MONTH", "opened_date")).orderBy("sqft", "id")
w3 = Window.orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("sqft").desc())

valid = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-08-26").cast("date"))
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.concat("city", F.lit(" "), "store_name").alias("city_store_name"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

with_windows = (
    valid
    .filter(F.col("city") == "Bangkok")
    .select(
        "id",
        "city",
        F.avg("sqft").over(w).alias("sqft_avg_2"),
        F.row_number().over(w2).alias("seq_no"),
        F.sum("sqft").over(w3).alias("cumulative_sqft"),
        F.dense_rank().over(w4).alias("sqft_dense_rank"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.product_id").isin([2, 4, 7])), "left_anti")
)

eligible_stores = (
    eligible
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id").isin([2, 4, 5, 6])), "left_anti")
)

filtered = (
    eligible_stores
    .filter(F.col("sqft_avg_2") > 15802)
    .select("*", F.trim("city").alias("clean_city"))
)

summary = (
    filtered
    .groupBy(
        F.when(F.col("sqft_avg_2") >= 13575, "PREMIUM").otherwise("STANDARD").alias("sqft_avg_2_tier"),
    )
    .agg(
        F.max("seq_no").alias("max_seq_no"),
        F.avg(F.when(F.col("city") == "Bangkok", F.col("cumulative_sqft"))).alias("bangkok_avg_cumulative_sqft"),
        F.avg(F.when(F.col("city") == "Tokyo", F.col("sqft_dense_rank"))).alias("tokyo_avg_sqft_dense_rank"),
        F.percentile_approx("seq_no", 0.9).alias("seq_no_p90"),
    )
)

result = (
    summary
    .select("sqft_avg_2_tier", "max_seq_no", "bangkok_avg_cumulative_sqft")
    .orderBy(F.col("bangkok_avg_cumulative_sqft").desc())
)
