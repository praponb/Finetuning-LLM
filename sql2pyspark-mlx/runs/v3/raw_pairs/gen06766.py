from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(F.countDistinct("cost").alias("unique_categorys"))
    .filter(F.col("unique_categorys") > 6)
    .select("brand", "unique_categorys")
)
