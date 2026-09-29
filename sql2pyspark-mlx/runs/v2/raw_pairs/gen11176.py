from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_stores.alias("s"), F.col("ss.store_id") == F.col("s.id"), "inner")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.supplier_id").isin([17, 21, 39]))
    .select(
        F.col("ss.id"),
        F.col("ss.units"),
        F.col("s.store_name"),
        F.col("p.category"),
        F.col("p.cost"),
    )
    .orderBy(F.col("cost").desc())
)
