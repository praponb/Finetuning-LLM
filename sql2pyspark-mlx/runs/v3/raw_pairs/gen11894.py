from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee") <= 13)
    .select(
        "id",
        "channel",
        F.add_months("order_date", 6).alias("order_date_plus_months"),
        (F.col("discount") + F.col("shipping_fee")).alias("discount_plus_shipping_fee"),
    )
    .orderBy(F.col("id").desc(), F.col("order_date_plus_months").desc())
    .limit(5)
)
