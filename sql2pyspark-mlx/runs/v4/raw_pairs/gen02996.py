from pyspark.sql import functions as F

kept = (
    df_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.cost").isNotNull()), "left_anti")
)

result = (
    kept
    .select("id", "customer_id", "store_id", "amount", "discount")
    .orderBy(F.col("customer_id").asc_nulls_last())
)
