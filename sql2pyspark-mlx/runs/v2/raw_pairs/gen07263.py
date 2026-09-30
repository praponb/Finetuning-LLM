from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") < 67)
    .select("id", "shipped_date", "status")
)
