from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "region")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.sum(F.when(F.col("city") == "London", F.col("sqft")).otherwise(0)).alias("london_sqft"),
    )
    .orderBy("region_values", "opened_month", "region")
    .limit(5)
)
