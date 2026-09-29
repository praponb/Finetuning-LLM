from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "UK")
    .groupBy("city", "is_active")
    .agg(
        F.sum(F.when(F.col("country") == "US", F.col("credit_limit")).otherwise(0)).alias("us_credit_limit"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
    .filter(F.col("us_credit_limit") >= 36)
)
