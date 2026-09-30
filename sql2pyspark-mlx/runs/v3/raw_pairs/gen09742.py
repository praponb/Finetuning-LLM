from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.when(F.col("s.sqft") >= 14871, "high").when(F.col("s.sqft") >= 7891, "mid").otherwise("low").alias("sqft_bucket"),
        F.col("s.city"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("o.channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("o.status")))).alias("status_values"),
    )
    .filter(F.col("pct_mobile") > 59)
    .orderBy("sqft_bucket")
)
