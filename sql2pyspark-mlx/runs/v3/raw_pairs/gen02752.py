from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter((F.col("p.stock_qty") < 287) & (F.col("s.rating") >= 2.7))
    .select(
        F.col("s.supplier_name"),
        F.col("s.country"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("p.cost"),
    )
)
