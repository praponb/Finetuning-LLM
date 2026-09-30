from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([10, 27]))
    .select("id", "order_date")
    .unionByName(
        df_orders
        .filter(F.col("status") == "PENDING")
        .select("id", "order_date")
    )
    .orderBy("id")
)
