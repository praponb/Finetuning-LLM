from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-08-01", "2025-04-01"))
    .select("id", "order_id")
    .unionByName(
        df_shipments
        .filter(F.col("status").isin(["IN_TRANSIT", "DELIVERED"]))
        .select("id", "order_id")
    )
    .distinct()
)
