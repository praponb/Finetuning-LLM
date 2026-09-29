from pyspark.sql import functions as F

base = df_products.filter((F.col("category") == "Grocery") | (F.size("tags") > 2))

launch_quarter_summary = (
    base
    .groupBy(F.quarter("launch_date").alias("launch_quarter"))
    .agg(F.countDistinct("cost").alias("unique_categorys"))
)

result = (
    launch_quarter_summary
    .filter(F.col("unique_categorys") < 9)
    .select("launch_quarter", "unique_categorys")
    .orderBy(F.col("launch_quarter").desc())
)
