from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") != "Electronics") & (F.col("price") > 1100) & F.col("product_name").like("Co%"))
    .groupBy("brand", "supplier_id")
    .agg(F.count("*").alias("num_products"))
    .orderBy(F.col("num_products").desc())
)
