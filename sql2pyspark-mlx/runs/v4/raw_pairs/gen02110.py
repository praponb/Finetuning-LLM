from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isin(["COMPLETED", "SHIPPED"]) | (F.col("order_date") <= "2024-08-03"))
    .select(
        "id",
        "shipping_fee",
        F.date_trunc("year", "order_date").alias("order_date_period"),
    )
)
