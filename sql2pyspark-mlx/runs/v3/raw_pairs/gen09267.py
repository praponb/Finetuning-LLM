from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", "segment")
    .agg(
        F.sum("age").alias("total_age"),
        F.min("age").alias("min_age"),
    )
    .filter(F.col("min_age") > 39)
    .orderBy(F.col("min_age").desc())
)
