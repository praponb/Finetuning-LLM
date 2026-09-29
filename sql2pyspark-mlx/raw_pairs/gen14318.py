from pyspark.sql import functions as F

dim_totals = (
    df_payments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .groupingSets(
        [["channel", "method"], ["channel"], []],
        F.col("d.channel").alias("channel"),
        F.col("e.method").alias("method"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("e.amount")), 2).alias("avg_amount"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    dim_totals
    .select(
        "method",
        "avg_amount",
        F.when(F.col("method").rlike("^(paypal|cash)$"), "EXTENDED").when(F.col("method").rlike("^(card|bank_transfer)$"), "OTHER_ZONE").otherwise("CORE").alias("method_zone"),
    )
    .orderBy(F.col("method").asc_nulls_first())
)
