from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") >= "2024-12-01")
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "LOST", 1).otherwise(0))) / F.count("*")), 1).alias("pct_lost"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
    .filter(F.col("pct_lost") > 66)
)
