from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("stock_qty") != 272).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(F.col("s.rating") >= 2.7)
    .select(F.col("s.id"), F.col("s.city"), F.col("s.rating"), F.col("s.supplier_name"))
    .orderBy("id")
)
