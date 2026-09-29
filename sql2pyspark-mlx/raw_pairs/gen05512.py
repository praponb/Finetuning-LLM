from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").between(640, 1000) | (F.col("price") > 500))
    .groupBy(
        F.when(F.col("brand").isin(["Initech", "Umbrella"]), "group_a").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.countDistinct("price").alias("unique_brands"),
        F.avg("cost").alias("avg_cost"),
    )
)
