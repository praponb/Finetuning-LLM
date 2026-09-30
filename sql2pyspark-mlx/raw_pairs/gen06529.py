from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 2)
    .select("revenue")
    .intersect(
        df_products
        .filter(F.col("stock_qty") < 342)
        .select(F.col("cost").alias("revenue"))
    )
)
