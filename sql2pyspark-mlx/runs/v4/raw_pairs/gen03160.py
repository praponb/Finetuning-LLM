from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("p.supplier_id") == F.col("s.id"), "inner")
    .groupBy(
        F.when(F.col("s.country").isin(["JP", "DE"]), "tier_1").otherwise("other").alias("country_group"),
        F.col("s.country"),
    )
    .agg(F.round(F.avg(F.col("p.price")), 2).alias("avg_price"))
    .filter(F.col("avg_price") >= 27)
    .orderBy("country_group")
)
