from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.filter(F.col("rating") >= 2.6).alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left_semi")
    .filter(F.col("products.category") == "Home")
    .select(
        F.col("products.id"),
        F.col("products.category"),
        F.col("products.product_name"),
        F.col("products.cost"),
    )
)
