from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id", "status")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)
