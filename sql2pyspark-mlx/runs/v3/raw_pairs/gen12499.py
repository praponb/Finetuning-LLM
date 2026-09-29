from pyspark.sql import functions as F

result = (
    df_shipments
    .select("status", "shipped_date", "carrier")
    .orderBy("status")
)
