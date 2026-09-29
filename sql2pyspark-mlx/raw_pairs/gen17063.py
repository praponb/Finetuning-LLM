from pyspark.sql import functions as F

result = (
    df_shipments
    .withColumn("cost_tier", F.when(F.col("cost") >= 94, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("MONTH", "delivered_date"))
    .cube("cost_tier", "activity_month")
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.max(F.when(F.col("carrier") == "DHL", F.col("cost"))).alias("dhl_max_cost"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.count("*").alias("num_rows"),
        F.grouping("cost_tier").alias("grouping_id"),
    )
    .orderBy("cost_stddev")
)
