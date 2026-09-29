from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").between(570, 620))
    .groupBy("category", F.month("launch_date").alias("launch_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
    )
    .filter(F.col("pct_acme") < 34)
    .select("category", "launch_month", "pct_acme")
    .orderBy(F.col("pct_acme").desc())
)
