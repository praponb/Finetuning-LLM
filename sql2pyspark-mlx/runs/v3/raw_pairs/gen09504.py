from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.col("order_id").isin([5, 18, 25]) | F.col("cost").isNotNull())
    .withColumn("status_prefix", F.substring("status", 1, 4))
)

summary = (
    src
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)

result = (
    summary
    .select("status", "carrier_list")
    .orderBy(F.col("status").desc())
    .limit(50)
)
