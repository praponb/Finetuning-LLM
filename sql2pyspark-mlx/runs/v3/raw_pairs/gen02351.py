from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .select("id", "channel", "order_date")
    .unionByName(
        df_orders
        .filter(F.col("status") == "SHIPPED")
        .select("id", "channel", "order_date")
    )
    .distinct()
    .orderBy("channel")
)
