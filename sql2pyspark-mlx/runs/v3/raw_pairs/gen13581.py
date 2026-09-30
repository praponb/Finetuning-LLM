from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") >= 17) & (F.col("channel") == "partner") & (F.col("shipping_fee") < 30))
    .select(
        "id",
        "customer_id",
        F.round((F.col("shipping_fee") * 1.15), 2).alias("adjusted_shipping_fee"),
        F.date_format("order_date", "MMM yyyy").alias("order_date_label"),
    )
    .orderBy("customer_id", "id")
    .limit(3)
)
