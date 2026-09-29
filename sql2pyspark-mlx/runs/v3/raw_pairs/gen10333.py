from pyspark.sql import functions as F

recent_shipments = df_shipments.filter(F.month("delivered_date").isin([2, 5, 6]))

agg = (
    recent_shipments
    .groupBy(
        F.when(F.col("carrier").isin(["Kerry", "Flash"]), "core").otherwise("other").alias("carrier_group"),
    )
    .agg(F.round(F.avg("cost"), 2).alias("avg_cost"))
)

result = agg
