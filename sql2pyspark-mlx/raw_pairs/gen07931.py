from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") < 12000) | ~F.col("country").isin(["DE", "TH", "UK"]))
    .select(
        "is_active",
        "segment",
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)
