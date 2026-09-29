from pyspark.sql import functions as F

filtered = (
    df_payments
    .filter(F.col("paid_at") > F.lit("2023-11-09 08:00:00").cast("timestamp"))
    .withColumn("method_group", F.when(F.col("method").isin(["card", "bank_transfer"]), "Group A").otherwise("Other"))
    .withColumn("status_upper", F.upper("status"))
    .withColumn("resolved_method", F.coalesce("method", F.lit("NONE")))
)

result = (
    filtered
    .alias("src")
    .join(df_orders.alias("p"), F.col("src.order_id") == F.col("p.id"), "inner")
    .groupBy(
        F.col("p.channel").alias("channel"),
        F.col("p.status").alias("order_status"),
        F.col("src.status").alias("payment_status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("src.amount")), 2).alias("total_amount"),
    )
    .orderBy(F.col("payment_status").desc_nulls_first())
)
