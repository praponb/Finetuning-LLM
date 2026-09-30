from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(~F.col("s.city").isin(["Chiang Mai", "London", "Berlin"]))
    .select(F.col("s.rating"), F.col("s.city"), F.col("p.cost"), F.col("p.brand"))
    .orderBy("city", "brand")
)
