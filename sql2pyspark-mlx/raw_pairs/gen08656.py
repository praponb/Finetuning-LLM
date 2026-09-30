from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "brand",
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.countDistinct("stock_qty").alias("unique_suppliers"),
    )
    .orderBy("launch_cohort")
)
