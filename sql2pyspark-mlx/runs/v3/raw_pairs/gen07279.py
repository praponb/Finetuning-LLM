from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
        F.count("cost").alias("cost_count"),
    )
    .filter(F.col("pct_acme") > 62)
    .select("category", "pct_acme", "cost_count")
    .orderBy("pct_acme")
)
