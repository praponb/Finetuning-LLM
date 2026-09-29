from pyspark.sql import functions as F

result = (
    df_customers
    .agg(
        F.sum(F.when(F.col("country") == "US", F.col("age")).otherwise(0)).alias("us_age"),
    )
    .orderBy(F.col("us_age").desc())
)
