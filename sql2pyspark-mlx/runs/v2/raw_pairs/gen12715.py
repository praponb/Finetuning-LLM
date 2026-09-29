from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Electronics") | (F.col("supplier_id") == 2))
    .agg(
        F.sum(F.when(F.col("category") == "Electronics", F.col("stock_qty")).otherwise(0)).alias("electronics_stock_qty"),
    )
)
