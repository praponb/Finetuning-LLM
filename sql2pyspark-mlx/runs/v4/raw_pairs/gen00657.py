from pyspark.sql import functions as F

src = df_shipments.filter((F.col("carrier") != "FedEx") & F.col("order_id").isNotNull())

shipments_stats = (
    src
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.count("cost").alias("cost_count"),
    )
)

result = shipments_stats.select("status", "cost_count")
