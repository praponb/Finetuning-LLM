from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.month("launch_date").isin([4, 7, 11]))
    .select("brand", "category")
    .distinct()
    .orderBy(F.col("category").desc())
)
