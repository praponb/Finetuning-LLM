from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Yoga%"))
    .groupBy(
        F.when(F.col("brand").isin(["Initech", "Acme"]), "group_a").otherwise("other").alias("brand_group"),
    )
    .agg(F.avg("cost").alias("avg_cost"))
    .filter(F.col("avg_cost") >= 370)
    .orderBy(F.col("brand_group").desc())
)
