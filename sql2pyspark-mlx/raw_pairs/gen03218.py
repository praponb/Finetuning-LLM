from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2024-06-05", "2025-04-01"))
    .select("store_id", "status")
    .unionByName(
        df_orders
        .filter(F.col("store_id").isin([1, 15, 26]))
        .select("store_id", "status")
    )
    .distinct()
)
