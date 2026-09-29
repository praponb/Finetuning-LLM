from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNull())
    .select("channel", "customer_id")
    .unionByName(
        df_orders
        .filter(F.col("order_date") > "2024-08-03")
        .select("channel", "customer_id")
    )
    .orderBy(F.col("customer_id").desc())
)
