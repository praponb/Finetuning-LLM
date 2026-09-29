from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower("email").like("%enr%") & (F.col("segment") == "SMB"))
    .groupBy("city", "country")
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.sum(F.when(F.col("country") == "US", F.col("credit_limit")).otherwise(0)).alias("us_credit_limit"),
    )
    .filter(F.col("us_credit_limit") < 63)
)
