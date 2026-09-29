from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0))) / F.count("*")), 1).alias("pct_globex"),
    )
    .filter(F.col("avg_cost") > 21)
    .orderBy("category")
)
