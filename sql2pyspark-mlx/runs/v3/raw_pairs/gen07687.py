from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.filter(~F.col("city").isin(["Singapore", "New York"])).alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left_semi")
    .select(F.col("p.id"), F.col("p.price"), F.col("p.cost"), F.col("p.launch_date"))
    .orderBy(F.col("cost").desc(), F.col("price").desc())
)
