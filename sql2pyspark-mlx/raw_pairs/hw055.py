from pyspark.sql import functions as F

result = df_customers.select(
    "id",
    F.when((F.col("segment") == "Corporate") & F.col("is_active"), 0.20)
    .when(F.col("segment") == "SMB", 0.10)
    .otherwise(0.0)
    .alias("discount_rate"),
)
