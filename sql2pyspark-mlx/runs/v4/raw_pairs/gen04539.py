from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 14)
    .select("id", "customer_id", "store_id")
    .unionByName(
        df_orders
        .filter(F.col("discount").between(0.35, 0.38))
        .select("id", "customer_id", "store_id")
    )
    .distinct()
    .orderBy("id")
)
