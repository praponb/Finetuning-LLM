from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.size("tags") > 1)
    .groupBy(
        F.when(F.col("cost") >= 650, "high").when(F.col("cost") >= 190, "mid").otherwise("low").alias("cost_bucket"),
        "brand",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Stark", 1).otherwise(0))) / F.count("*")), 1).alias("pct_stark"),
    )
    .filter(F.col("pct_stark") < 55)
    .orderBy(F.col("brand").desc(), "cost_bucket")
)
