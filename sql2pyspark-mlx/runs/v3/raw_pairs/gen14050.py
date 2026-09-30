from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(F.col("rating") < 4.0).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .filter(F.col("p.product_name").like("%Off%"))
    .select(F.col("p.id"), F.col("p.price"), F.col("p.product_name"), F.col("p.cost"))
    .orderBy("product_name", "id")
    .limit(20)
)
