from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.month("delivered_date").alias("delivered_month"), "status")
    .agg(
        F.max("cost").alias("max_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
    )
)
