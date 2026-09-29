from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(((F.col("carrier") != "DHL") & (F.col("shipped_date") > "2024-07-11")) | (F.col("delivered_date") > "2025-08-01"))
)
