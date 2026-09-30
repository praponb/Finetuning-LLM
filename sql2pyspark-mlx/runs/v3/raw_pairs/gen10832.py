from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 105, "high").when(F.col("cost") >= 49, "mid").otherwise("low").alias("cost_bucket"),
        "order_id",
        F.when(F.col("carrier").isin(["DHL", "FedEx"]), "group_a").otherwise("other").alias("carrier_group"),
    )
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
    .filter(F.col("stddev_cost") > 53)
    .orderBy(F.col("order_id").desc())
)
