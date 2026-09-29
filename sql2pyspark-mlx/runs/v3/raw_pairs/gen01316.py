from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["RETURNED", "PENDING", "SHIPPED"]))
    .select("id", "order_date", "store_id")
    .intersect(
        df_orders
        .filter(F.col("amount") > 1020)
        .select("id", "order_date", "store_id")
    )
)
