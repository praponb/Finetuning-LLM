from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("amount").isNotNull() & F.col("amount").between(1400, 1560)) | (F.col("order_date") < "2023-03-01"))
    .orderBy("channel", "id")
    .limit(10)
)
