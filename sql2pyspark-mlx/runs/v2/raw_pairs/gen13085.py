from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(~F.col("category").isin(["Home", "Grocery", "Fashion"])).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .select(F.col("s.id"), F.col("s.country"), F.col("s.city"), F.col("s.rating"))
    .orderBy(F.col("rating").desc(), "id")
    .limit(20)
)
