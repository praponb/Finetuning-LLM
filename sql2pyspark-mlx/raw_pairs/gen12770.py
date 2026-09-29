from pyspark.sql import functions as F

eligible = (
    df_stores
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") < F.col("s.opened_date")), "left_anti")
)

result = (
    eligible
    .groupBy(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count(F.when(F.col("region") == "East", 1)).alias("east_rows"),
        F.percentile_approx("sqft", 0.9).alias("sqft_p90"),
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.bit_and("sqft").alias("sqft_bit_and"),
        F.percentile_approx("sqft", 0.95).alias("sqft_p95"),
    )
    .filter(F.col("num_cities") >= 1)
    .orderBy(F.col("num_cities").asc_nulls_last())
)
