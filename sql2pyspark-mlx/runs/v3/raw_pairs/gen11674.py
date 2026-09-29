from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.year("delivered_date") == 2024) & F.col("carrier").isNull() & F.col("cost").between(34, 58))
    .groupBy("status", F.quarter("shipped_date").alias("shipped_quarter"), "carrier")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("total_count"),
    )
)
