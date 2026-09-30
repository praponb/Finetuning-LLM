from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("brand").isin(["Stark", "Umbrella", "Globex"])).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_anti")
    .filter(F.col("s.rating").isNull())
    .select(F.col("s.id"), F.col("s.country"), F.col("s.city"), F.col("s.rating"))
)
