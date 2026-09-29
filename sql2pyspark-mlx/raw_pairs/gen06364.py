from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("status").isin(["LOST", "IN_TRANSIT"]) & (F.col("delivered_date") >= "2023-06-01")) | ~F.col("carrier").isin(["DHL", "FedEx", "Flash"]))
    .withColumn("status_prefix", F.substring("status", 1, 3))
)

agg = (
    valid_shipments
    .groupBy(
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.count(F.when(F.col("status") == "RETURNED", 1)).alias("returned_count"),
    )
)

result = agg.orderBy(F.col("returned_count").desc())
