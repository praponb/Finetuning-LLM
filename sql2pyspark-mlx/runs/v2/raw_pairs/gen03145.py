from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.lower("product_name").like("%apt%") | (F.col("cost") < 710))
    .select(
        "brand",
        "supplier_id",
        "category",
        F.greatest("stock_qty", "price").alias("greatest_stock_qty_price"),
    )
    .orderBy("category")
)
