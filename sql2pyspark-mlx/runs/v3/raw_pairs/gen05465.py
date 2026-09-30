from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "left")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .filter(F.col("ss.revenue") < 1300)
    .groupBy(
        F.col("p.category"),
        F.when(F.col("p.brand").isin(["Acme", "Initech"]), "tier_1").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.round(F.avg(F.col("ss.revenue")), 2).alias("avg_revenue"),
        F.min(F.col("ss.sale_date")).alias("first_sale_date"),
    )
    .orderBy("brand_group")
)
