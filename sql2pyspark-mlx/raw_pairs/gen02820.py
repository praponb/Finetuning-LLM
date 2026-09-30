from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
        F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0)).alias("berlin_count"),
    )
    .filter(F.col("berlin_count") >= 70)
    .orderBy("berlin_count", "signup_month")
    .limit(5)
)
