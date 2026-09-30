from pyspark.sql import functions as F

store_sales_by_product = (
    df_store_sales
    .filter(F.col("revenue").between(1700, 2800))
    .groupBy("product_id")
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.count("*").alias("num_store_sales"),
    )
)

result = (
    df_products.alias("products")
    .join(store_sales_by_product.alias("StoreSalesByProduct"), F.col("products.id") == F.col("StoreSalesByProduct.product_id"), "inner")
    .select(
        F.col("products.id"),
        F.col("products.price"),
        F.col("products.cost"),
        F.col("StoreSalesByProduct.avg_revenue"),
        F.col("StoreSalesByProduct.num_store_sales"),
    )
    .orderBy(F.col("num_store_sales").desc(), "cost", "id")
    .limit(10)
)
