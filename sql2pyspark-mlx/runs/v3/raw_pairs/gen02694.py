from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("segment"))).alias("segment_values"),
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
    .filter(F.col("total_credit_limit") >= 28000)
    .drop("total_credit_limit")
)
