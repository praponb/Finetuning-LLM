from pyspark.sql import functions as F

filtered_products = (
    df_products
    .filter(F.col("brand").isin(["Initech", "Globex"]) & F.col("category").isin(["Electronics", "Home", "Grocery"]) & F.upper("product_name").like("%OFF%"))
)

summary = (
    filtered_products
    .groupBy("category")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
)

result = (
    summary
    .filter(F.col("pct_home") < 26)
    .orderBy(F.col("pct_home").desc(), "category")
    .limit(5)
)
