from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") > 0.14)
    .select("id", "customer_id", "order_date")
    .intersect(
        df_orders
        .filter(F.col("channel").isin(["store", "web", "mobile"]))
        .select("id", "customer_id", "order_date")
    )
    .orderBy("customer_id")
)
