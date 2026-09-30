from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.avg(F.col("p.amount")).alias("avg_amount"),
        F.round(((100 * F.sum(F.when(F.col("p.method") == "card", 1).otherwise(0))) / F.count("*")), 1).alias("pct_card"),
    )
    .orderBy(F.col("pct_card").desc())
)
