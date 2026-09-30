from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("launch_date") <= "2025-07-31") & (F.col("supplier_id") == 16) & (F.col("supplier_id") == 32))
    .groupBy("category")
    .agg(
        F.sum(F.when(F.col("category") == "Electronics", F.col("price")).otherwise(0)).alias("electronics_price"),
        F.sum("stock_qty").alias("total_stock_qty"),
    )
    .orderBy("category")
)
