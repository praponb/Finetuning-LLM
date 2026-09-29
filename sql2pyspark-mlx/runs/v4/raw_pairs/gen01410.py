from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .select(
        F.col("suppliers.city"),
        F.col("products.supplier_id"),
        F.col("products.price"),
        F.col("products.category"),
        F.col("products.cost").cast("int").alias("cost_int"),
    )
    .orderBy(F.col("supplier_id").desc())
)
