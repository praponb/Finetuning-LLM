from pyspark.sql import functions as F

excluded_suppliers = df_suppliers.filter(F.col("rating").between(2.1, 3.2)).select("id")

result = (
    df_products.alias("products")
    .join(excluded_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left_anti")
    .filter(F.col("products.supplier_id").isNotNull() | F.lit(excluded_suppliers.isEmpty()))
    .select(
        F.col("products.id"),
        F.col("products.cost"),
        F.col("products.stock_qty"),
        F.col("products.supplier_id"),
    )
)
