from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("L%"))
    .groupBy(
        F.when(F.col("cost") >= 610, "high").when(F.col("cost") >= 160, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(F.countDistinct("cost").alias("unique_brands"))
    .filter(F.col("unique_brands") < 5)
    .select("cost_bucket", "unique_brands")
    .orderBy("cost_bucket", "unique_brands")
)
