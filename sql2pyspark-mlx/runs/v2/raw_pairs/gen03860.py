from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 19)
    .groupBy(
        F.date_trunc("MONTH", "delivered_date").alias("delivered_month"),
        F.year("delivered_date").alias("delivered_year"),
    )
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.avg("cost").alias("avg_cost"),
    )
)
