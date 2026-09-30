from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.min("launch_date").alias("first_launch_date"),
    )
    .orderBy("launch_year")
    .limit(3)
)
