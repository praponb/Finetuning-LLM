from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "outer")
    .filter(F.col("p.price").between(650, 1060))
    .groupBy(
        F.when(F.col("p.brand").isin(["Globex", "Stark"]), "core").otherwise("other").alias("brand_group"),
    )
    .agg(F.max(F.col("ss.sale_date")).alias("latest_sale_date"))
    .orderBy("brand_group")
)
