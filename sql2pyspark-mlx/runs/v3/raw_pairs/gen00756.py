from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") >= "2025-04-09") & (F.col("shipped_date") < "2024-10-01"))
    .groupBy(F.month("delivered_date").alias("delivered_month"))
    .agg(F.sort_array(F.collect_set("carrier")).alias("carrier_list"))
    .orderBy("delivered_month")
)
