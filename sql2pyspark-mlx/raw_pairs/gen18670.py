from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .filter(F.col("suppliers.city").isin(["New York", "Bangkok"]) & F.col("products.price").between(1090, 1130))
    .select(
        F.col("products.product_name"),
        F.col("products.cost"),
        F.col("products.brand"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.rating"),
    )
)
