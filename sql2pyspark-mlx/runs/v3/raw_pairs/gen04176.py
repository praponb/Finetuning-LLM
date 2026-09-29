from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower("email").like("%mma%"))
    .groupBy("city")
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
    )
    .filter(F.col("min_credit_limit") >= 13400)
    .select("city", "min_credit_limit", "segment_values")
)
