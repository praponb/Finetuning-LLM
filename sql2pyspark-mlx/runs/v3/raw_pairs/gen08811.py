from pyspark.sql import functions as F

result = (
    df_products
    .cube(F.date_format("launch_date", "yyyy-MM").alias("launch_month"), "brand")
    .agg(
        F.max("cost").alias("max_cost"),
        F.countDistinct("price").alias("unique_categorys"),
        F.max("launch_date").alias("last_launch_date"),
    )
)
