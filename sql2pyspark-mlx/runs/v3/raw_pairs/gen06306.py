from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(
        F.when(F.col("o.channel").isin(["mobile", "store"]), "group_a").otherwise("other").alias("channel_group"),
        F.col("o.channel"),
    )
    .agg(
        (F.max(F.col("p.amount")) - F.min(F.col("p.amount"))).alias("amount_range"),
        F.sort_array(F.collect_set(F.col("p.method"))).alias("method_list"),
    )
    .filter(F.col("amount_range") > 42)
    .orderBy(F.col("channel_group").desc(), F.col("amount_range").desc(), "channel")
    .limit(5)
)
