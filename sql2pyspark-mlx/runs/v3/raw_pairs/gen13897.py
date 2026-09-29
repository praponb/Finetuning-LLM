from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2025-04-01")
    .select("id", "carrier", "shipped_date")
    .unionByName(
        df_shipments
        .filter(F.col("status") != "LOST")
        .select("id", "carrier", "shipped_date")
    )
)
