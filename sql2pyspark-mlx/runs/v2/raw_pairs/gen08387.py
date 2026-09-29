from pyspark.sql import functions as F

base = df_products.filter(F.col("launch_date") < "2025-07-28")

agg = (
    base
    .groupBy("brand", "category")
    .agg(
        F.countDistinct("cost").alias("unique_categorys"),
        F.countDistinct("price").alias("unique_brands"),
    )
)

result = (
    agg
    .filter(F.col("unique_brands") > 6)
    .select("brand", "category", "unique_categorys", "unique_brands")
    .orderBy("unique_brands")
)
