from pyspark.sql import functions as F

result = (
    df_suppliers.alias("suppliers")
    .join(df_products.alias("products"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .filter(F.col("suppliers.city").isin(["New York", "Bangkok", "London"]))
    .select(
        F.col("suppliers.supplier_name"),
        F.col("suppliers.city"),
        F.col("suppliers.country"),
        F.col("products.stock_qty"),
    )
    .orderBy(F.col("city").desc())
)
