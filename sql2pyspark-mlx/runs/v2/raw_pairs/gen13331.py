from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.date_trunc("MONTH", "shipped_date").alias("shipped_month"),
        "status",
        "carrier",
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
