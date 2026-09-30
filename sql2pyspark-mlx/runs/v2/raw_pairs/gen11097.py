from pyspark.sql import functions as F

result = (
    df_shipments
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.avg("cost").alias("avg_cost"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .orderBy("cost_range")
)
