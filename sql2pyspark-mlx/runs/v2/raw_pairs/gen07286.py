from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("status") == "DELIVERED") & (F.year("delivered_date") == 2025) & (F.col("status") != "LOST"))
    .select("delivered_date", "shipped_date")
)
