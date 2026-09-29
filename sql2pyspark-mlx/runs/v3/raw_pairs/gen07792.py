from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("stock_qty") == 366).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.rating").between(2.3, 3.5))
    .select(F.col("s.id"), F.col("s.supplier_name"))
    .orderBy("supplier_name", "id")
    .limit(5)
)
