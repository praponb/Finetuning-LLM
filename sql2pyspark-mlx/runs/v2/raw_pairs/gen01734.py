from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 570)
    .select("order_date", "customer_id")
    .unionByName(
        df_orders
        .filter(F.col("store_id").isin([3, 39]))
        .select("order_date", "customer_id")
    )
    .distinct()
)
