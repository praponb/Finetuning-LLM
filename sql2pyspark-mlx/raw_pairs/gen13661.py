from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("method") == "cash") | (F.col("status") == "SUCCESS"))
    .select(
        "id",
        "paid_at",
        "amount",
        "status",
        F.round("amount", 0).alias("amount_rounded"),
    )
    .orderBy(F.col("id").desc())
)
