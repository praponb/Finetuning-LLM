from pyspark.sql import functions as F

src = df_shipments.filter(F.col("order_id").isin([23, 24]))

agg = (
    src
    .groupBy("carrier")
    .agg(
        F.max("cost").alias("max_cost"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)

result = (
    agg
    .filter(F.col("max_cost") < 51)
    .select("carrier", "max_cost", "carrier_list")
    .orderBy(F.col("max_cost").desc(), F.col("carrier").desc())
)
