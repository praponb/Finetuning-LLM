from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(~F.col("category").isin(["Fashion", "Books", "Grocery"])).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(~F.col("s.country").isin(["UK", "DE"]))
    .select(F.col("s.id"), F.col("s.rating"), F.col("s.city"), F.col("s.supplier_name"))
)
