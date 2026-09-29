from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.quarter("shipped_date").alias("shipped_quarter"))
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.min("cost").alias("min_cost"),
    )
    .filter(F.col("min_cost") < 59)
)
