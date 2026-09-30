from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(F.col("price").isNotNull()).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .filter(F.upper(F.trim(F.col("s.supplier_name"))) == "TOKYO TRADERS")
    .select(F.col("s.id"), F.col("s.country"), F.col("s.city"))
    .orderBy("city")
)
