from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter(F.col("order_id").isin([6, 22, 38]) | F.col("cost").between(67, 68))
    .withColumn("clean_status", F.trim("status"))
)

carrier_summary = (
    valid_shipments
    .groupBy("carrier")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = carrier_summary
