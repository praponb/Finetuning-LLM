from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .filter(F.length(F.col("suppliers.supplier_name")) > 5)
    .select(
        F.col("suppliers.rating"),
        F.col("suppliers.city"),
        F.col("products.category"),
        F.col("products.brand"),
        F.col("products.price"),
    )
    .orderBy("category", F.col("rating").desc())
)
