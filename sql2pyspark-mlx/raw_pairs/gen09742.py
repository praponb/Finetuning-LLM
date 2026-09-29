from pyspark.sql import functions as F

result = (
    df_products
    .select("cost")
    .union(
        df_store_sales
        .filter(F.col("product_id").isin([2, 4, 6, 9]))
        .select("revenue")
    )
    .distinct()
)
