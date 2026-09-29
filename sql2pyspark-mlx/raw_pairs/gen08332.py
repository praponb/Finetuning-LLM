from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.channel") == "mobile")
    .groupBy(F.col("o.channel"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
    )
    .filter(F.col("pct_failed") >= 76)
    .orderBy("channel")
)
