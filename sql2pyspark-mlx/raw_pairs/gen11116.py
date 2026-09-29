from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") < "2024-12-01") & F.col("order_id").isin([3, 9, 23, 40]))
    .groupBy(
        F.when(F.col("carrier").isin(["DHL", "Kerry"]), "core").otherwise("other").alias("carrier_group"),
    )
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.count(F.when(F.col("carrier") == "UPS", 1)).alias("ups_count"),
    )
)
