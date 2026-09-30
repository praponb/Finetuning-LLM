from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([5, 6, 12]) & ((F.col("amount") < 1330) | ~F.col("status").isin(["COMPLETED", "CANCELLED"])))
    .select("id", "shipping_fee", "discount")
    .orderBy("shipping_fee")
)
