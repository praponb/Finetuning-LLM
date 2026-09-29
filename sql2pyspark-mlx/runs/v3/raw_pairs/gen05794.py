from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Stark", "Globex", "Acme"]))
    .groupBy(
        F.when(F.col("price") >= 1010, "high").when(F.col("price") >= 400, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
        F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0)).alias("globex_count"),
        F.round(((100 * F.sum(F.when(F.col("category") == "Sports", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sports"),
    )
)
