from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("order_id").isin([13, 23, 40]) & ~F.col("method").isin(["card", "bank_transfer"])) | F.col("order_id").isNotNull())
    .agg(
        F.count("*").alias("total_count"),
        F.countDistinct("amount").alias("unique_orders"),
    )
    .orderBy(F.col("unique_orders").desc())
)
