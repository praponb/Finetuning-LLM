from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") < "2024-09-11")
    .select(
        "id",
        "credit_limit",
        "name",
        "is_active",
        F.when(F.col("segment").isin(["Corporate", "Consumer"]), "Priority").otherwise("Other").alias("segment_group"),
    )
)
