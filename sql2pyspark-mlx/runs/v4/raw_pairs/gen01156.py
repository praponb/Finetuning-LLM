from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") > "2023-06-01")
    .select("shipped_date", "order_id")
)
