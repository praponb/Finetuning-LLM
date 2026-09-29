from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("shipped_date") == 2024)
    .select("cost", "shipped_date", "delivered_date")
)
