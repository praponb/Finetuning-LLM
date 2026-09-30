from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)

filtered = (
    df_payments
    .filter((F.col("paid_at") >= F.lit("2023-07-13").cast("date")) & (F.col("status") == "FAILED"))
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        F.round((F.col("amount") * 1.15), 2).alias("amount_scaled"),
        F.coalesce("method", F.lit("NONE")).alias("resolved_method"),
    )
)

result = (
    filtered
    .select(
        "id",
        "order_id",
        "amount",
        "paid_at",
        "status",
        "amount_scaled",
        "resolved_method",
        F.avg("amount").over(w_recent).alias("last4_avg_amount"),
        F.max("amount").over(w_recent).alias("last4_max_amount"),
        F.min("amount").over(w_recent).alias("last4_min_amount"),
    )
)
