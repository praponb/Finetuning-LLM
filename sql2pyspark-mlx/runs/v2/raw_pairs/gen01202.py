from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("s.delivered_date") > "2025-09-01")
    .groupBy(F.col("o.channel"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("s.carrier") == "Kerry", 1).otherwise(0))) / F.count("*")), 1).alias("pct_kerry"),
    )
    .orderBy(F.col("pct_kerry").desc())
)
