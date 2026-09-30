from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") < 0.15) & (F.col("shipping_fee") >= 35) & F.col("status").isin(["PENDING", "COMPLETED", "RETURNED"]))
    .select(
        "id",
        "shipping_fee",
        "customer_id",
        "discount",
        F.date_format("order_date", "yyyy-MM").alias("order_date_label"),
        (F.col("discount") + F.col("shipping_fee")).alias("discount_plus_shipping_fee"),
    )
    .orderBy(F.col("discount").desc())
)
