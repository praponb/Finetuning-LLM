from pyspark.sql import functions as F

with_dims = (
    df_payments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.amount").alias("order_amount"), F.col("o.channel"))
)

result = (
    with_dims
    .groupBy(F.date_trunc("year", "paid_at").alias("year_start"))
    .agg(
        F.countDistinct("method").alias("distinct_methods"),
        F.percentile_approx("amount", 0.95).alias("amount_p95"),
        F.sum(F.when(F.col("channel") == "partner", F.col("order_amount")).otherwise(0)).alias("partner_order_amount"),
    )
    .filter(F.col("distinct_methods") < 1)
)
