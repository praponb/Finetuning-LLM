from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 22)
    .select("id", "status", "order_date")
    .unionByName(
        df_orders
        .filter(F.col("amount") > 420)
        .select("id", "status", "order_date")
    )
    .orderBy(F.col("id").desc())
)
