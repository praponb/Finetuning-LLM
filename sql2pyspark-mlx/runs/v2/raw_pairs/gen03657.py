from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 590, "high").when(F.col("cost") >= 110, "mid").otherwise("low").alias("cost_bucket"),
        "category",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
    )
    .orderBy(F.col("globex_count").desc(), "cost_bucket", "category")
    .limit(50)
)
