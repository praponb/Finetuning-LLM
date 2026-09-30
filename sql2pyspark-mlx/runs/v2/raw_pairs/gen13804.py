from pyspark.sql import functions as F

result = (
    df_products
    .filter(~(F.array_contains("tags", "eco") & (F.col("price") >= 310)))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Home", 1).otherwise(0))) / F.count("*")), 1).alias("pct_home"),
    )
)
