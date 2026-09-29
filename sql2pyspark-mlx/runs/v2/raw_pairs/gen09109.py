from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("price") < 550).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.rating").between(2.4, 3.2))
    .select(F.col("s.id"), F.col("s.rating"))
    .orderBy("id")
)
