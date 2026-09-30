from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        "status",
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
    )
    .agg(
        F.count(F.when(F.col("status") == "LOST", 1)).alias("lost_count"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)
