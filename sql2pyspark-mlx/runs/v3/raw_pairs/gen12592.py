from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").isNotNull())
    .select(
        "id",
        "segment",
        "name",
        "credit_limit",
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
    .orderBy("id")
)
