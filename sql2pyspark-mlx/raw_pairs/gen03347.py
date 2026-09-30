from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["store", "mobile", "partner"]))
    .select("order_date")
    .intersect(
        df_orders
        .filter(F.col("order_date") <= "2024-10-01")
        .select("order_date")
    )
)
