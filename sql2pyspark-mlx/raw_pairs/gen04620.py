from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 93, "HIGH").otherwise("LOW").alias("cost_tier"),
        "status",
    )
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.count_if(F.col("status") == "IN_TRANSIT").alias("in_transit_events"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("in_transit_events") < 1000)
)
