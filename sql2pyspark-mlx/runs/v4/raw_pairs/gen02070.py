from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("carrier").isin(["DHL", "Kerry"]), "tier_1").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.min("cost").alias("min_cost"),
        F.count("cost").alias("cost_count"),
    )
    .filter(F.col("cost_count") >= 4)
)
