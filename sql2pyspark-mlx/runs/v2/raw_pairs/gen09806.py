from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("paid_at") >= "2024-08-01") & (F.col("method") == "bank_transfer"))
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.trim("method").alias("clean_method"),
    )
    .orderBy(F.col("id").desc(), F.col("status").desc())
)
