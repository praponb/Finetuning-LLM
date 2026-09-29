from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.min("credit_limit").alias("min_credit_limit"),
        F.countDistinct("age").alias("unique_segments"),
    )
    .filter(F.col("unique_segments") < 2)
    .drop("unique_segments")
)
