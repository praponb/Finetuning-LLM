from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 5)
    .select("status")
    .distinct()
)
