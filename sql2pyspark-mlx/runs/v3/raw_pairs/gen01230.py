from pyspark.sql import functions as F

base = df_products.filter(F.col("brand").isNull() & (F.col("supplier_id") == 32))

launch_cohort_summary = (
    base
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("brand"))).alias("brand_values"))
)

result = (
    launch_cohort_summary
    .select("launch_cohort", "brand_values")
    .orderBy(F.col("brand_values").desc())
)
