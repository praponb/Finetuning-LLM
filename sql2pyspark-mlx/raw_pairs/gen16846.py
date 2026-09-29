from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("method").orderBy("paid_at", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("status")
w_recent = Window.orderBy(F.col("amount").desc(), "id")
amount_by_order_id = df_payments.groupBy("order_id").agg(F.max("amount").alias("max_payment_amount"))

scored = (
    df_payments
    .select(
        "*",
        F.avg("amount").over(w_grp).alias("amount_avg_2_prior"),
        F.avg("amount").over(w2).alias("amount_avg_5_prior"),
        F.round((F.col("amount") / F.sum("amount").over(w3)), 4).alias("amount_share"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

result = (
    scored
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "paid_at",
        "status",
        "amount_avg_2_prior",
        "amount_avg_5_prior",
        "amount_share",
        F.when(F.col("status").rlike("^(FAILED|SUCCESS)$"), "CORE").otherwise("GROUP_B").alias("status_zone"),
        F.col("max_payment_amount"),
    )
)
