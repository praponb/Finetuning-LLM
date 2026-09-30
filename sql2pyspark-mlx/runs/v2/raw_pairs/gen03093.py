from pyspark.sql import functions as F

base = df_shipments.filter((F.col("cost") < 41) | F.col("cost").isNotNull())

agg = (
    base
    .groupBy("status")
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)

result = (
    agg
    .select("status", "avg_cost")
    .orderBy("avg_cost")
)
