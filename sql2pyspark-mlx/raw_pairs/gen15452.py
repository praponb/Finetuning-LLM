from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount").between(800, 1390) & F.col("method").isin(["card", "promptpay"]))
    .select(
        "id",
        "paid_at",
        "method",
        F.substring("method", 1, 4).alias("method_prefix"),
    )
    .orderBy(F.col("method").desc())
)
