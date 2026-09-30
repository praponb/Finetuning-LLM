from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .select(
        F.col("products.id"),
        F.col("products.product_name"),
        F.col("products.launch_date"),
        F.col("suppliers.supplier_name"),
        F.col("suppliers.city"),
        F.col("suppliers.rating"),
    )
    .orderBy(F.col("city").desc(), "id")
    .limit(10)
)
