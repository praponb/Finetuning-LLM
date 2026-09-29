from pyspark.sql import functions as F

by_dimension = (
    df_payments
    .alias("a")
    .join(df_orders.alias("o"), F.col("a.order_id") == F.col("o.id"), "inner")
    .groupingSets(
        [["order_status", "payment_status"], ["order_status"]],
        F.col("o.status").alias("order_status"),
        F.col("a.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("a.amount")).alias("min_amount"),
    )
)

result = (
    by_dimension
    .select(
        "order_status",
        "payment_status",
        "num_rows",
        F.when(F.col("payment_status").rlike("^(FAILED|REFUNDED)$"), "CORE").otherwise("PRIORITY").alias("payment_status_zone"),
    )
    .orderBy(F.col("order_status").desc(), "payment_status")
)
