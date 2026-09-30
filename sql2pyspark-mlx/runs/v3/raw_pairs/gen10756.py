from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "carrier",
        "order_id",
        F.date_trunc("MONTH", "delivered_date").alias("delivered_month"),
    )
    .agg(
        F.sum("cost").alias("total_cost"),
        F.countDistinct("cost").alias("unique_orders"),
    )
    .filter(F.col("unique_orders") > 4)
)
