from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2024-12-01")
    .groupBy(
        F.date_format("delivered_date", "yyyy-MM").alias("delivered_month"),
        "carrier",
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.max("cost").alias("max_cost"),
    )
    .filter(F.col("max_cost") < 30)
    .select("delivered_month", "carrier", "status_list", "max_cost")
    .orderBy(F.col("max_cost").desc())
)
