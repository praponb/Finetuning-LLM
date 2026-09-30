from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(~F.col("carrier").isin(["UPS", "Kerry", "FedEx"]) & (F.col("cost") >= 93))
    .withColumn("carrier_title", F.initcap(F.trim("carrier")))
)

summary = (
    src
    .groupBy(
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
        "carrier",
    )
    .agg(
        F.countDistinct("cost").alias("unique_carriers"),
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
    )
)

result = (
    summary
    .filter(F.col("dhl_count") > 4)
    .orderBy(F.col("dhl_count").desc())
)
