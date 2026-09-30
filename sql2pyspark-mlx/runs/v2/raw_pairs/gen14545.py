from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "DELIVERED"]))
    .groupBy("order_id")
    .agg(F.sort_array(F.collect_set("carrier")).alias("carrier_list"))
)
