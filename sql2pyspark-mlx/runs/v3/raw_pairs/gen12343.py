from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2025-05-01", "2025-08-01"))
    .select("shipped_date", "order_id")
    .unionByName(
        df_shipments
        .filter(F.col("status") != "DELIVERED")
        .select("shipped_date", "order_id")
    )
)
