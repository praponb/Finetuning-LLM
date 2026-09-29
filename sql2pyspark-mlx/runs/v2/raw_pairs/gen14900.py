from pyspark.sql import functions as F

result = (
    df_payments
    .select(
        "id",
        "paid_at",
        "amount",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.date_trunc("WEEK", "paid_at").alias("paid_at_period"),
    )
)
