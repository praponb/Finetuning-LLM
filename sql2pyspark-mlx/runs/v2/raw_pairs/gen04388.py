from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("status") != "FAILED") | (F.col("amount") > 950))
    .select(
        "id",
        "paid_at",
        F.col("amount").cast("int").alias("amount_int"),
        F.when(F.col("amount") >= 1290, "High").when(F.col("amount") >= 730, "Medium").otherwise("Low").alias("amount_tier"),
    )
)
