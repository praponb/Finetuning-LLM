from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.min("age").alias("min_age"),
    )
    .filter(F.col("corporate_count") >= 5)
    .select("country", "corporate_count", "min_age")
)
