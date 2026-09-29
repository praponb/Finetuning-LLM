from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") <= 0.14)
    .select(
        "id",
        "discount",
        F.quarter("order_date").alias("order_date_quarter"),
        F.ceil("discount").alias("discount_whole"),
    )
    .orderBy("discount_whole", "order_date_quarter")
)
