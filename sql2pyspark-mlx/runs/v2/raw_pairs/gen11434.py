from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "Kerry")
    .groupBy("status", "carrier")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.max("delivered_date").alias("latest_delivered_date"),
    )
)
