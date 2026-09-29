from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "store")
    .select("status", "store_id")
    .unionByName(
        df_orders
        .filter(F.col("customer_id") == 30)
        .select("status", "store_id")
    )
    .distinct()
)
