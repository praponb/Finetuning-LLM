from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("p.price") < 360)
    .select(
        F.col("s.rating"),
        F.col("s.supplier_name"),
        F.col("s.city"),
        F.col("p.product_name"),
        F.col("p.stock_qty"),
    )
)
