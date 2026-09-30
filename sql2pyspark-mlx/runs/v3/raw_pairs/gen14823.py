from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2023-03-01", "2023-12-01"))
    .select("status")
    .unionByName(
        df_shipments
        .filter(F.col("shipped_date") < "2025-04-01")
        .select("status")
    )
    .distinct()
)
