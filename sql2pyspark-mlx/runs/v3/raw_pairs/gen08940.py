from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "RETURNED") & (F.col("cost") <= 49) & (F.col("shipped_date") <= "2023-11-01"))
)
