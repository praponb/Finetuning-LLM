from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") >= "2023-11-01")
    .select("delivered_date")
    .subtract(df_stores.filter(F.col("city") == "Singapore").select("opened_date"))
)
