from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.date_trunc("MONTH", "shipped_date").alias("shipped_month"),
        F.quarter("delivered_date").alias("delivered_quarter"),
        F.date_trunc("MONTH", "delivered_date").alias("delivered_month"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count("*").alias("total_count"),
    )
)
