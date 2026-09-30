from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") > 33)
    .select(
        "order_date",
        "customer_id",
        F.round((F.col("discount") * 1.1), 2).alias("adjusted_discount"),
    )
    .orderBy("customer_id", F.col("order_date").desc())
)
