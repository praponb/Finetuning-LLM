from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([5, 9, 10]))
    .select("revenue")
    .intersect(
        df_subscriptions
        .filter(F.col("customer_id").isin([5, 14]))
        .select("monthly_fee")
    )
    .orderBy(F.col("revenue").desc())
)
