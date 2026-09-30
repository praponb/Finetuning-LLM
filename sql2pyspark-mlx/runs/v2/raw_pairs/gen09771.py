from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.year("delivered_date") >= 2025)
    .select("status", "shipped_date")
    .orderBy("shipped_date")
)
