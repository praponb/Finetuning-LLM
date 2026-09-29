from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2023-03-01")
    .select(
        "id",
        "status",
        F.when(F.col("amount") >= 1720, "High").when(F.col("amount") >= 730, "Medium").otherwise("Low").alias("amount_tier"),
        F.when(F.col("method").isin(["bank_transfer", "promptpay"]), "Core").otherwise("Other").alias("method_group"),
    )
    .orderBy(F.col("id").desc())
    .limit(1)
)
