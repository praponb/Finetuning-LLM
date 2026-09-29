from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "left")
    .groupBy(
        F.col("p.supplier_id"),
        F.when(F.col("p.category").isin(["Fashion", "Grocery"]), "core").otherwise("other").alias("category_group"),
    )
    .agg(F.min(F.col("ss.sale_date")).alias("min_sale_date"))
    .orderBy(F.col("min_sale_date").desc(), "category_group")
)
