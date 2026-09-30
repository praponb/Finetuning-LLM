from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("price") >= 910, "high").when(F.col("price") >= 510, "mid").otherwise("low").alias("price_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("brand") == "Initech", 1).otherwise(0)).alias("initech_count"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("cost")).otherwise(0)).alias("electronics_cost"),
    )
    .filter(F.col("initech_count") > 80)
    .orderBy("electronics_cost")
)
