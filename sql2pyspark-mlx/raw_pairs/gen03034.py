from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("order_date") >= "2023-11-01") & F.col("shipping_fee").isNull() & ~F.col("status").isin(["COMPLETED", "PENDING"]))
    .select(
        "id",
        "shipping_fee",
        "store_id",
        (F.col("discount") - F.col("amount")).alias("discount_minus_amount"),
    )
    .orderBy(F.col("store_id").desc())
)
