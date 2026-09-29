from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "outer")
    .select(
        F.col("products.cost"),
        F.col("suppliers.city"),
        F.col("suppliers.rating"),
        F.col("suppliers.supplier_name"),
    )
)
