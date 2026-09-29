from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNotNull() & F.col("status").isin(["LOST", "RETURNED"]))
    .groupBy("carrier")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
    .orderBy("carrier")
)
