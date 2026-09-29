from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter(F.month("delivered_date").isin([6, 11, 12]) & F.col("status").isin(["LOST", "DELIVERED"]))
    .withColumn("shipped_date_plus_5d", F.date_add("shipped_date", 5))
)

carrier_group_summary = (
    valid_shipments
    .groupBy(
        F.when(F.col("carrier").isin(["DHL", "FedEx"]), "tier_1").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
    )
)

result = carrier_group_summary.select("carrier_group", "status_values")
