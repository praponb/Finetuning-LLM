from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("category") == "Home") & F.array_contains("tags", "new"))
    .groupBy("category")
    .agg(F.avg("cost").alias("avg_cost"))
    .filter(F.col("avg_cost") >= 280)
    .orderBy("category")
    .limit(20)
)
