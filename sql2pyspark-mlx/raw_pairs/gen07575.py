from pyspark.sql import functions as F

by_dimension = (
    df_payments
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .groupingSets(
        [["order_status", "method"], ["order_status"]],
        F.col("d.status").alias("order_status"),
        F.col("s.method").alias("method"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("s.amount")).alias("total_amount"),
    )
)

result = (
    by_dimension
    .select(
        "order_status",
        "method",
        "rows_total",
        "total_amount",
        F.when(F.col("order_status").rlike("^(SHIPPED|CANCELLED)$"), "EXTENDED").otherwise("GROUP_A").alias("order_status_zone"),
    )
)
