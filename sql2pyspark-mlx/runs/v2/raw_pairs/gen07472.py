from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(~F.col("brand").isin(["Acme", "Globex", "Umbrella"])).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.col("s.city").isin(["Chiang Mai", "Singapore", "Berlin"]))
    .select(F.col("s.id"), F.col("s.country"), F.col("s.city"))
    .orderBy(F.col("id").desc())
)
