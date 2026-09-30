from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select(
        "id",
        "credit_limit",
        "name",
        F.substring("name", 1, 2).alias("name_prefix"),
        F.trim("segment").alias("clean_segment"),
    )
)
