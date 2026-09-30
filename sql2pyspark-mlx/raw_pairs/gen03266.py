from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.shipping_fee").between(22, 35))
    .groupBy(F.col("o.status"), F.year(F.col("o.order_date")).alias("order_year"))
    .agg(F.countDistinct(F.col("s.cost")).alias("unique_carriers"))
    .orderBy(F.col("order_year").desc(), F.col("unique_carriers").desc())
)
