from pyspark.sql import functions as F

filtered_shipments = df_shipments.filter(F.year("delivered_date") >= 2024)

summary = (
    filtered_shipments
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("carrier")).alias("carrier_list"))
)

result = summary.select("status", "carrier_list")
