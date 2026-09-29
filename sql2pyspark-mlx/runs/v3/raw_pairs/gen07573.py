from pyspark.sql import functions as F

result = (
    df_products
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Electronics", 1).otherwise(0))) / F.count("*")), 1).alias("pct_electronics"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
        F.max("cost").alias("max_cost"),
    )
)
