from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("segment", "city", "country")
    .agg(
        F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0)).alias("corporate_count"),
    )
    .orderBy("country")
)
