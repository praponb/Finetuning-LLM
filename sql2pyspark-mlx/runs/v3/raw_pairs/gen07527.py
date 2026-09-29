from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.channel") != "mobile")
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(
        F.sort_array(F.collect_set(F.col("p.status"))).alias("status_list"),
        F.round(((100 * F.sum(F.when(F.col("p.method") == "paypal", 1).otherwise(0))) / F.count("*")), 1).alias("pct_paypal"),
    )
    .orderBy(F.col("channel").desc())
)
