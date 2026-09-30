from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.length("product_name") >= 13)
    .agg(
        F.max("launch_date").alias("last_launch_date"),
        F.min("cost").alias("min_cost"),
    )
)
