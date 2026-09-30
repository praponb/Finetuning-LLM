from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") < 670)
    .groupBy(F.date_format("launch_date", "yyyy-MM").alias("launch_month"))
    .agg(
        F.avg("price").alias("avg_price"),
        F.countDistinct("cost").alias("unique_categorys"),
    )
    .filter(F.col("unique_categorys") < 7)
    .select("launch_month", "avg_price", "unique_categorys")
)
