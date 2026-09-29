from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select("id", "customer_id", "order_date")
    .intersect(
        df_orders
        .filter(F.col("order_date") > "2023-07-01")
        .select("id", "customer_id", "order_date")
    )
)
