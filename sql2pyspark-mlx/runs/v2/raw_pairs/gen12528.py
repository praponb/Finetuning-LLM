from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") < "2023-09-14")
    .select("status", "shipped_date")
    .intersect(
        df_shipments
        .filter(F.col("status").isin(["DELIVERED", "RETURNED"]))
        .select("status", "shipped_date")
    )
)
