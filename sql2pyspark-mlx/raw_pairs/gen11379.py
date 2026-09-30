from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.to_date("paid_at") >= "2025-06-19")
    .select("amount")
    .intersect(
        df_store_sales
        .filter(F.col("product_id").isin([6, 7]))
        .select(F.col("revenue").alias("amount"))
    )
    .orderBy(F.col("amount").desc())
)
