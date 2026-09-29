from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_store_sales.alias("ss"), F.col("p.id") == F.col("ss.product_id"), "left")
    .filter(F.col("p.supplier_id") == 25)
    .select(
        F.col("p.brand"),
        F.col("ss.sale_date"),
        F.trim(F.col("p.category")).alias("clean_category"),
    )
    .orderBy("clean_category")
)
