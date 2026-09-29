from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment") == "Corporate")
    .select(
        "id",
        "age",
        "credit_limit",
        F.substring("segment", 1, 3).alias("segment_prefix"),
        F.lower("name").alias("name_lower"),
    )
)
