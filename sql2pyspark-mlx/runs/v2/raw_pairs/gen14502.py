from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isNull())
    .groupBy("city", "country")
    .agg(
        F.count("age").alias("age_count"),
        F.sum("age").alias("total_age"),
        F.count("*").alias("row_count"),
    )
)
