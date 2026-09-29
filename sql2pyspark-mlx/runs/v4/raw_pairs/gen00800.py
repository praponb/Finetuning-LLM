from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter((F.year("signup_date") >= 2023) & ~F.col("is_active"))
    .withColumn("signup_date_plus_27d", F.date_add("signup_date", 27))
)

customers_stats = (
    filtered_customers
    .groupBy("segment")
    .agg(
        F.min("age").alias("min_age"),
        F.countDistinct("age").alias("unique_citys"),
    )
)

result = customers_stats
