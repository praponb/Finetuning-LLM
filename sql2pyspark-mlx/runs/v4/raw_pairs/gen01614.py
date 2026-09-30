from pyspark.sql import functions as F

result = (
    df_store_sales.alias("store_sales")
    .join(df_products.alias("products"), F.col("store_sales.product_id") == F.col("products.id"), "inner")
    .join(df_suppliers.alias("suppliers"), F.col("products.supplier_id") == F.col("suppliers.id"), "left")
    .groupBy(
        F.when(F.col("products.category").isin(["Grocery", "Fashion"]), "group_a").otherwise("other").alias("category_group"),
    )
    .agg(F.sum(F.col("store_sales.revenue")).alias("total_revenue"))
)
