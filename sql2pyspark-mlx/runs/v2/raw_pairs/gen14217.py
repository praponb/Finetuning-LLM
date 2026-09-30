from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0))) / F.count("*")), 1).alias("pct_initech"),
    )
    .filter(F.col("pct_initech") >= 53)
    .select("category", "pct_initech")
    .orderBy(F.col("category").desc())
)
