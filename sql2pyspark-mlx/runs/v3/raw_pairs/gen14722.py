from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id").isNull() & F.col("carrier").isin(["FedEx", "UPS", "Kerry"])) | (F.col("carrier") == "DHL"))
    .groupBy("carrier")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.countDistinct("cost").alias("unique_carriers"),
    )
)
