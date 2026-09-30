from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(29, 32))
    .select("status", "order_date")
    .intersect(
        df_orders
        .filter(F.col("amount") >= 590)
        .select("status", "order_date")
    )
    .orderBy("order_date")
)
