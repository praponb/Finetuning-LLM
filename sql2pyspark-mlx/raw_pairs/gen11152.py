from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(F.avg("price").alias("avg_price"))
    .filter(F.col("avg_price") < 630)
    .orderBy("avg_price")
)
