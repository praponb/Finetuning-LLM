from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("ss.product_id") == F.col("p.id"), "inner")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.city") != "London")
    .groupBy(
        F.when(F.col("s.country").isin(["SG", "US"]), "tier_1").otherwise("other").alias("country_group"),
    )
    .agg((F.max(F.col("ss.units")) - F.min(F.col("ss.units"))).alias("units_range"))
    .filter(F.col("units_range") >= 40)
    .orderBy(F.col("units_range").desc(), "country_group")
    .limit(3)
)
