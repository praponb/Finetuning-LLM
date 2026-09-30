from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") >= 660)
    .groupBy("category")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0))) / F.count("*")), 1).alias("pct_stark"),
    )
    .orderBy("category")
    .limit(5)
)
