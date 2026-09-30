from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .filter(F.col("o.store_id") == 23)
    .groupBy(F.col("c.segment"))
    .agg(
        F.countDistinct(F.col("s.cost")).alias("unique_statuss"),
        F.round(((100 * F.sum(F.when(F.col("s.status") == "IN_TRANSIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_in_transit"),
    )
    .orderBy(F.col("unique_statuss").desc(), "segment")
    .limit(10)
)
