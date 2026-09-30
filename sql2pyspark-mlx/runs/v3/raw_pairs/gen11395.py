from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("carrier").isin(["UPS", "Flash"]), "tier_1").otherwise("other").alias("carrier_group"),
        "carrier",
    )
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
)
