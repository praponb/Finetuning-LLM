from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .groupBy("carrier")
    .agg(F.countDistinct("cost").alias("unique_orders"))
    .filter(F.col("unique_orders") >= 5)
    .select("carrier", "unique_orders")
)
