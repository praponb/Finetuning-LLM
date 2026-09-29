from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country").isin(["SG", "UK"]))
    .groupBy("is_active")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
    .orderBy(F.col("tokyo_count").desc())
)
