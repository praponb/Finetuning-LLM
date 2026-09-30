from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .filter(~F.col("o.channel").isin(["store", "web"]))
    .groupBy(
        F.col("o.channel"),
        F.when(F.col("o.amount") >= 1700, "high").when(F.col("o.amount") >= 510, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
    )
    .orderBy(F.col("pct_refunded").desc())
)
