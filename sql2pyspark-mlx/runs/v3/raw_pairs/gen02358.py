from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["Flash", "DHL", "UPS"]))
    .groupBy(
        "status",
        F.when(F.col("carrier").isin(["Kerry", "UPS"]), "group_a").otherwise("other").alias("carrier_group"),
    )
    .agg(F.avg("cost").alias("avg_cost"))
)
