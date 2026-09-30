from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("p.category"))
    .agg(
        F.max(F.col("ss.sale_date")).alias("latest_sale_date"),
        F.round(F.stddev(F.col("ss.units")), 2).alias("stddev_units"),
    )
    .orderBy(F.col("stddev_units").desc(), "category")
    .limit(5)
)
