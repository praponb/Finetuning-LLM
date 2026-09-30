from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price") <= 170)
    .select("unit_price")
    .union(
        df_subscriptions
        .filter(F.col("start_date") >= "2024-06-14")
        .select("monthly_fee")
    )
    .orderBy("unit_price")
)
