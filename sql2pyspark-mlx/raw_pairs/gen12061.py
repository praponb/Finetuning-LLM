from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") != "RETURNED")
    .select("id", "cost", "shipped_date", "carrier")
)
