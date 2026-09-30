from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "LOST")
    .orderBy(F.col("order_id").desc(), "id")
    .limit(5)
)
