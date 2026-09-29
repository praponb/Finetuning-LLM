from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.store_id").isNull())
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.when(F.col("s.carrier") == "Kerry", 1)).alias("kerry_count"),
        (F.max(F.col("s.cost")) - F.min(F.col("s.cost"))).alias("cost_range"),
    )
    .filter(F.col("kerry_count") >= 3)
)
