from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .filter(F.col("ss.units") < 15)
    .select(
        F.col("ss.units"),
        F.col("p.supplier_id"),
        F.col("p.cost"),
        F.initcap(F.trim(F.col("p.category"))).alias("category_title"),
    )
)
