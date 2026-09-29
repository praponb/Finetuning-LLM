from pyspark.sql import functions as F

enriched = (
    df_payments
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.status").alias("order_status"))
)

result = (
    enriched
    .select(
        "id",
        "order_id",
        "paid_at",
        "status",
        F.when(F.col("status").rlike("^(FAILED|SUCCESS)$"), "CORE").when(F.col("status").rlike("^(REFUNDED)$"), "GROUP_A").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy("order_id")
)
