from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~F.col("status").isin(["REFUNDED", "SUCCESS", "FAILED"]) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "status",
        F.trim("method").alias("clean_method"),
        F.when(F.col("amount") >= 1460, "High").when(F.col("amount") >= 450, "Medium").otherwise("Low").alias("amount_tier"),
    )
    .orderBy(F.col("order_id").desc())
)
