from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier") == "DHL")
    .select("id", "status", "order_id", "carrier")
    .orderBy("carrier")
)
