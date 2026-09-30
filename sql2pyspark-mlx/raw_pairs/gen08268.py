from pyspark.sql import functions as F

base = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-06-11 01:00:00").cast("timestamp")) & F.col("order_id").isin([20, 39]))
    .select(
        "id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.initcap(F.trim("method")).alias("method_title"),
    )
)

result = (
    base
    .select("id", "status")
    .subtract(
        df_payments
        .filter(F.hour("paid_at").between(6, 11))
        .select("id", "status")
    )
)
