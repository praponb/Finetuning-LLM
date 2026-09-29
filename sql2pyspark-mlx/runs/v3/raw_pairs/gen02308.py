from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .orderBy("order_date", "id")
    .limit(20)
)
