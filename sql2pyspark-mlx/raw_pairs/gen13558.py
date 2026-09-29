from pyspark.sql import functions as F

base = df_products.filter((F.col("cost") >= 650) & (F.col("launch_date") < "2025-02-01"))

summary = (
    base
    .groupBy(
        "brand",
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
    )
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        (F.max("price") - F.min("price")).alias("price_range"),
    )
)

result = (
    summary
    .filter(F.col("price_range") > 22)
    .select("brand", "launch_cohort", "brand_list")
)
