from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") <= "2023-11-01")
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("brand") == "Acme", F.col("stock_qty")).otherwise(0)).alias("acme_stock_qty"),
    )
    .orderBy("brand", "acme_stock_qty")
)
