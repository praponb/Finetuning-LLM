from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") < 0.19) & ((F.col("order_date") <= "2024-05-01") | F.col("status").isin(["SHIPPED", "COMPLETED", "RETURNED"])))
    .select("id", "shipping_fee", "status")
    .orderBy("id")
    .limit(20)
)
