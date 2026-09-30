from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .filter(F.col("o.status").isin(["CANCELLED", "PENDING"]))
    .groupBy(F.col("s.city"))
    .agg(
        F.avg(F.col("o.shipping_fee")).alias("avg_shipping_fee"),
        F.countDistinct(F.col("o.amount")).alias("unique_stores"),
    )
    .filter(F.col("unique_stores") > 5)
)
