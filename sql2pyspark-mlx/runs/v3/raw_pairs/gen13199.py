from pyspark.sql import functions as F

excluded_suppliers = df_suppliers.filter(F.col("city").isin(["Chiang Mai", "Singapore"])).select("id")

result = (
    df_products.alias("p")
    .join(excluded_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_anti")
    .filter(F.col("p.supplier_id").isNotNull() | F.lit(excluded_suppliers.isEmpty()))
    .select(F.col("p.id"), F.col("p.brand"), F.col("p.launch_date"))
    .orderBy(F.col("brand").desc(), F.col("launch_date").desc())
)
