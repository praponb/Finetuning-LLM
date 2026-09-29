from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "amount",
        "status",
        "paid_at",
        F.round("amount", 1).alias("amount_rounded"),
    )
    .orderBy(F.col("amount").desc())
)
