from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "store"]))
    .select("id", "store_id", "order_date")
    .unionByName(
        df_orders
        .filter(F.col("order_date").between("2023-10-24", "2025-07-01"))
        .select("id", "store_id", "order_date")
    )
    .orderBy(F.col("id").desc())
)
