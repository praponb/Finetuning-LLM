from pyspark.sql import functions as F

result = (
    df_products.alias("products")
    .join(df_suppliers.alias("suppliers"), F.col("suppliers.id") == F.col("products.supplier_id"), "left")
    .groupBy(F.col("suppliers.city"))
    .agg(F.count("*").alias("cnt"))
    .orderBy("city")
)
