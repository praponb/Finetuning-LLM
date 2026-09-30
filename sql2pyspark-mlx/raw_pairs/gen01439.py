from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.countDistinct("stock_qty").alias("unique_categorys"),
        F.min("launch_date").alias("min_launch_date"),
    )
    .filter(F.col("unique_categorys") > 7)
    .select("brand", "unique_categorys", "min_launch_date")
)
