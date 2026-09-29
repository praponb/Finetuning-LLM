from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "inner")
    .filter(~((F.col("p.category") != "Books") & (F.col("p.category") == "Fashion")))
    .select(
        F.col("ss.sale_date"),
        F.col("p.cost"),
        F.col("p.launch_date"),
        F.when(F.col("p.brand").isin(["Stark", "Acme"]), "Core").otherwise("Other").alias("brand_group"),
    )
)
