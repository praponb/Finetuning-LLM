from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("category", F.year("launch_date").alias("launch_year"))
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.min("cost").alias("min_cost"),
    )
)
