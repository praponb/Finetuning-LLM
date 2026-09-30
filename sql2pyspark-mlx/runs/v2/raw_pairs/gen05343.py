from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 3)
    .drop("total_count")
)
