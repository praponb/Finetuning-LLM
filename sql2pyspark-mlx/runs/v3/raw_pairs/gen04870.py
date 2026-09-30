from pyspark.sql import functions as F

base = df_shipments.filter(F.col("status") == "DELIVERED")

summary = (
    base
    .groupBy("carrier")
    .agg(
        F.max("delivered_date").alias("latest_delivered_date"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)

result = (
    summary
    .select("carrier", "carrier_list")
    .orderBy("carrier")
)
