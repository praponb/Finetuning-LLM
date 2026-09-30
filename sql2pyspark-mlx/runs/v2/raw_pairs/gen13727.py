from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() & (F.col("cost") <= 89))
    .groupBy(F.month("delivered_date").alias("delivered_month"))
    .agg(
        F.count("*").alias("shipment_count"),
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
    )
    .filter(F.col("shipment_count") < 4)
)
