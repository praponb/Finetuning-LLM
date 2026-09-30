from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("Ba%"))
    .select("cost")
    .subtract(
        df_transactions
        .filter(F.col("txn_time") >= "2025-03-01")
        .select("amount")
    )
)
