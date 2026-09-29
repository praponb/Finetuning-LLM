from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("shipped_date") < "2025-03-16") & (F.col("order_id") == 20)) | (F.col("delivered_date") >= "2024-07-02"))
    .select("carrier")
)
