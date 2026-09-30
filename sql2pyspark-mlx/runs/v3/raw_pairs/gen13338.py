from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("category").isin(["Grocery", "Sports", "Electronics"])).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.supplier_name"))
    .orderBy("city", "id")
    .limit(10)
)
