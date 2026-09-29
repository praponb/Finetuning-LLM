from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.filter(~F.col("brand").isin(["Umbrella", "Acme"])).alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left_semi")
    .select(F.col("s.id"), F.col("s.city"), F.col("s.supplier_name"))
    .orderBy("city", "supplier_name", "id")
    .limit(20)
)
