from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter(F.col("carrier").isin(["FedEx", "DHL"]) & F.col("shipped_date").between("2025-01-31", "2025-10-06") & (F.col("order_id") == 15))
    .withColumn("shipped_date_period", F.date_trunc("MONTH", "shipped_date"))
)

agg = (
    filtered_shipments
    .groupBy("status", "carrier")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
    )
)

result = agg
