from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        "brand",
        "category",
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        (F.max("stock_qty") - F.min("stock_qty")).alias("stock_qty_range"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
    )
    .orderBy("launch_cohort", "brand")
)
