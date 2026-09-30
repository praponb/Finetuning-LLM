from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("stock_qty").between(123, 338))
    .select(
        "id",
        "supplier_id",
        "category",
        F.length("product_name").alias("product_name_length"),
    )
    .orderBy("supplier_id", "id")
    .limit(10)
)
