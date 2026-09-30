from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") < 95) & F.col("carrier").isin(["DHL", "FedEx"]) & F.col("order_id").isin([28, 31]))
    .groupBy("order_id", "status")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.sum(F.when(F.col("status") == "DELIVERED", 1).otherwise(0)).alias("delivered_count"),
    )
    .orderBy("order_id")
)
