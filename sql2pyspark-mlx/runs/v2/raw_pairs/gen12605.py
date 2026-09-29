from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") <= 7500) & ((F.lower(F.trim("name")) == "olivia garcia") | ~F.col("is_active")))
    .agg(
        F.avg("credit_limit").alias("avg_credit_limit"),
        F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0)).alias("tokyo_count"),
    )
    .orderBy(F.col("avg_credit_limit").desc(), "tokyo_count")
)
