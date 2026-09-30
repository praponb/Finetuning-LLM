from pyspark.sql import functions as F

filtered = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0).alias("is_in_transit"),
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_clean"),
    )
)

result = (
    filtered
    .groupBy("status")
    .pivot("carrier", ["DHL", "FedEx"])
    .agg(F.sum("is_in_transit").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "status",
        F.col("DHL_amount").alias("dhl_amount"),
        F.col("DHL_n").alias("dhl_n"),
        F.col("FedEx_amount").alias("fedex_amount"),
        F.col("FedEx_n").alias("fedex_n"),
    )
)
