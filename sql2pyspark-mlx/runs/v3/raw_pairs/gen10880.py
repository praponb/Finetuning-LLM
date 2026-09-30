from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "US") & (F.col("age") == 34))
    .select(
        "credit_limit",
        "segment",
        "city",
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)
