from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("stock_qty") >= 267).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(F.col("s.supplier_name").like("Tok%"))
    .select(F.col("s.id"), F.col("s.supplier_name"), F.col("s.rating"))
    .orderBy(F.col("rating").desc(), "id")
    .limit(10)
)
