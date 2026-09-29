from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("s.order_id").isNull())
    .groupBy(F.col("o.status"))
    .agg(
        F.countDistinct(F.col("s.cost")).alias("unique_statuss"),
        F.countDistinct(F.col("s.cost")).alias("unique_orders"),
    )
    .orderBy(F.col("unique_orders").desc(), "status")
    .limit(5)
)
