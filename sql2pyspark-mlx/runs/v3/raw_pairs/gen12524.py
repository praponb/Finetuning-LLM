from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("id", "customer_id", "order_date")
    .unionByName(
        df_orders
        .filter(F.col("amount") < 1330)
        .select("id", "customer_id", "order_date")
    )
)
