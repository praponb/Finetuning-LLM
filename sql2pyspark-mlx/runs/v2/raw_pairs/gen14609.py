from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") < "2025-05-01") | (F.year("shipped_date") >= 2025))
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)
