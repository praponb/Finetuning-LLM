from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("status")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.countDistinct("cost").alias("unique_orders"),
        (F.max("cost") - F.min("cost")).alias("cost_range"),
    )
)
