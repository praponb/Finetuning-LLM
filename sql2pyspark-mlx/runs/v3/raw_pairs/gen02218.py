from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2024-01-01")
    .select("carrier", "status")
    .unionByName(df_shipments.filter(F.col("cost") < 41).select("carrier", "status"))
)
