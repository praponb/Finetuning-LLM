from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "inner")
    .filter(~(F.col("suppliers.supplier_name").isNull() & (F.col("products.stock_qty") >= 125)))
    .select(
        F.col("products.id"),
        F.col("products.cost"),
        F.col("products.stock_qty"),
        F.col("suppliers.rating"),
    )
)
