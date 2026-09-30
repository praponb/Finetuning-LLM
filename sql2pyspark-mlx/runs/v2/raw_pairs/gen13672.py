from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id") == 27) | (F.col("status") != "RETURNED"))
    .agg(F.min("cost").alias("min_cost"))
)
