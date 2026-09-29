from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.countDistinct("price").alias("unique_brands"),
        F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"),
        F.min("price").alias("min_price"),
    )
)
