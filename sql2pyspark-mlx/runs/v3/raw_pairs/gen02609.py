from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("delivered_month"))
    .agg(
        F.avg("cost").alias("avg_cost"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
    .orderBy("avg_cost")
)
