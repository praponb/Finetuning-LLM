from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select("customer_id", "order_date")
    .unionByName(
        df_orders
        .filter(F.col("status") != "RETURNED")
        .select("customer_id", "order_date")
    )
    .distinct()
)
