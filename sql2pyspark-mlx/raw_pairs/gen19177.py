from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left_anti")
    .filter(F.col("suppliers.city").isin(["Tokyo", "Berlin"]))
    .select(
        F.col("suppliers.id"),
        F.col("suppliers.country"),
        F.col("suppliers.rating"),
    )
    .orderBy("id", "country")
)
