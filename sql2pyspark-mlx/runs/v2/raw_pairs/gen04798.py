from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id") == 30)
    .select("status", "customer_id")
    .unionByName(
        df_orders
        .filter(F.col("store_id") == 19)
        .select("status", "customer_id")
    )
    .distinct()
    .orderBy(F.col("customer_id").desc())
)
