from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id").isin([33, 40]) & (F.col("carrier") == "Kerry")) | (F.col("shipped_date") >= "2024-06-23"))
    .groupBy(F.date_format("shipped_date", "yyyy-MM").alias("shipped_month"))
    .agg(
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count("*").alias("shipment_count"),
    )
)
