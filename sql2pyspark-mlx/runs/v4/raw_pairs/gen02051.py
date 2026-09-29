from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status", "order_id")
    .agg(
        F.min("delivered_date").alias("min_delivered_date"),
        F.countDistinct("cost").alias("unique_orders"),
    )
    .filter(F.col("unique_orders") > 2)
    .drop("unique_orders")
)
