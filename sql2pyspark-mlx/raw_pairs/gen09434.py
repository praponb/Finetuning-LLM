from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "SHIPPED") | ~F.col("status").isin(["RETURNED", "PENDING"]))
    .select(
        "id",
        "shipping_fee",
        "discount",
        F.last_day("order_date").alias("order_date_month_end"),
        F.lower("status").alias("status_lower"),
    )
    .orderBy(F.col("discount").desc(), "order_date_month_end", "id")
    .limit(20)
)
