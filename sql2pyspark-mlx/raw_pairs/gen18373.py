from pyspark.sql import functions as F

result = (
    df_products.alias("p")
    .join(df_suppliers.alias("s"), F.col("s.id") == F.col("p.supplier_id"), "right")
    .groupBy(
        F.when(F.col("s.city").isin(["Bangkok", "Berlin"]), "tier_1").otherwise("other").alias("city_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
    )
)
