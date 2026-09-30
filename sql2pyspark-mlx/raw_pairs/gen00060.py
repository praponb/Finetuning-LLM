from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy(F.month("paid_at")).orderBy("amount", "id")

filtered = (
    df_payments
    .filter(F.col("order_id").isNotNull() & (~F.col("method").eqNullSafe(F.lit("cash"))))
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        F.initcap(F.trim("method")).alias("method_title"),
        F.coalesce("method", F.lit("N/A")).alias("method_filled"),
    )
)

result = (
    filtered
    .filter(F.hour("paid_at").between(7, 10))
    .select(
        "*",
        F.avg("amount").over(w).alias("amount_avg_6_prior"),
        F.lead("amount", 1).over(w2).alias("next_amount"),
    )
)
