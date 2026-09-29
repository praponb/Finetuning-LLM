from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("price")).otherwise(0)).alias("umbrella_price"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.max("price").alias("max_price"),
    )
    .filter(F.col("max_price") < 470)
    .drop("max_price")
)
