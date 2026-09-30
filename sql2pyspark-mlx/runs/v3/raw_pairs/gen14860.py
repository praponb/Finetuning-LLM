from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "category",
        "brand",
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
    .orderBy("brand", F.col("launch_cohort").desc())
)
