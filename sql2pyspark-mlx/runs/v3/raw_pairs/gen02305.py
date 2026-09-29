from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("price") >= 660) & (F.col("stock_qty") != 242))
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.min("launch_date").alias("min_launch_date"),
        F.max("launch_date").alias("latest_launch_date"),
        (F.max("price") - F.min("price")).alias("price_range"),
    )
    .filter(F.col("price_range") > 55)
)
