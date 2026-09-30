from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("category").isin(["Sports", "Books"]), "priority").otherwise("other").alias("category_group"),
    )
    .agg(F.avg("cost").alias("avg_cost"))
    .filter(F.col("avg_cost") >= 370)
    .select("category_group", "avg_cost")
    .orderBy("category_group")
    .limit(10)
)
