from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.max("launch_date").alias("latest_launch_date"),
        F.countDistinct("stock_qty").alias("unique_categorys"),
    )
    .filter(F.col("unique_categorys") >= 3)
    .drop("unique_categorys")
)
