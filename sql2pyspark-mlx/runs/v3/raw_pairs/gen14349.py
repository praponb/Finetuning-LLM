from pyspark.sql import functions as F

result = (
    df_shipments
    .rollup("order_id", "status")
    .agg(F.min("shipped_date").alias("min_shipped_date"))
)
