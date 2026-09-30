from pyspark.sql import functions as F

result = (
    df_products
    .rollup(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        "category",
    )
    .agg(F.countDistinct("cost").alias("unique_categorys"))
)
