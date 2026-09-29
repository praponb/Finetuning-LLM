from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("category"))).alias("category_values"),
        F.min("stock_qty").alias("min_stock_qty"),
    )
    .filter(F.col("min_stock_qty") < 310)
    .select("launch_cohort", "category_values", "min_stock_qty")
    .orderBy("category_values", "launch_cohort")
    .limit(5)
)
