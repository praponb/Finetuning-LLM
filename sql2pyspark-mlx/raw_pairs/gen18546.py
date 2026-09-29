from pyspark.sql import functions as F

result = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "left")
    .select(
        "a.id",
        "a.product_name",
        "a.brand",
        "a.stock_qty",
        "a.tags",
        "a.supplier_id",
        F.col("d.city"),
        F.col("d.country"),
    )
)
