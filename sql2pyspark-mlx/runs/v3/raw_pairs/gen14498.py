from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "inner")
    .groupBy(F.col("suppliers.city"))
    .agg(F.round(F.avg(F.col("products.cost")), 2).alias("avg_cost"))
    .orderBy(F.col("avg_cost").desc(), "city")
    .limit(5)
)
