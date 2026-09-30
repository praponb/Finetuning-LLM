from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("p.supplier_id").isNull() & (F.col("p.stock_qty") < 237))
    .select(F.col("p.category"), F.col("s.rating"))
    .orderBy(F.col("rating").desc(), F.col("category").desc())
)
