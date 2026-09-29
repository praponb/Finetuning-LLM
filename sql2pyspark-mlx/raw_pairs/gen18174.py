from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([5, 8, 9, 10]))
    .select("revenue")
    .union(
        df_subscriptions
        .filter(F.col("start_date").between("2024-12-01", "2025-05-01"))
        .select("monthly_fee")
    )
    .distinct()
)
