from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 630, "high").when(F.col("cost") >= 190, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Home", 1).otherwise(0)).alias("home_count"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
    .filter(F.col("home_count") < 56)
    .orderBy(F.col("cost_bucket").desc())
)
