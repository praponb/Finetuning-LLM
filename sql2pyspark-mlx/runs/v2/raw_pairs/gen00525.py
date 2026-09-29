from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 1)
    .select("id", "carrier", "status")
    .orderBy(F.col("carrier").desc())
)
