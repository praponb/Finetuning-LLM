from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 28)
    .select("status")
    .intersect(
        df_shipments
        .filter(F.col("carrier").isin(["FedEx", "Kerry", "UPS"]))
        .select("status")
    )
    .orderBy(F.col("status").desc())
)
