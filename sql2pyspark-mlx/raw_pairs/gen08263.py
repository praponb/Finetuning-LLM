from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.month("delivered_date").isin([1, 7, 8]) & F.col("carrier").isin(["UPS", "FedEx"]) & F.col("order_id").isin([7, 31, 32, 35]))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("N/A")))
)

agg = (
    src
    .groupBy(F.year("shipped_date").alias("shipped_year"), "status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.count("cost").alias("cost_count"),
    )
)

result = agg.orderBy("status")
