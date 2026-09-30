from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 39)
    .select("id", "status", "store_id")
    .unionByName(
        df_orders
        .filter(F.col("store_id") == 28)
        .select("id", "status", "store_id")
    )
    .distinct()
    .orderBy("id")
)
