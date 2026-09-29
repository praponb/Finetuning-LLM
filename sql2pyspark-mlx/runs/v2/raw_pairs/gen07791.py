from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") <= "2025-09-02")
    .select("delivered_date", "shipped_date", "status")
)
