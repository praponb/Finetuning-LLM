from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["mobile", "partner"]))
    .select("order_date", "customer_id")
    .unionByName(
        df_orders
        .filter(F.col("status").isin(["SHIPPED", "CANCELLED", "COMPLETED"]))
        .select("order_date", "customer_id")
    )
    .distinct()
    .orderBy("order_date")
)
