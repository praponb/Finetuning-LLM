from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        "product_id",
    )
    .agg(F.count("units").alias("units_count"))
    .filter(F.col("units_count") > 8)
    .select("sale_cohort", "product_id", "units_count")
    .orderBy("product_id", "sale_cohort")
    .limit(5)
)
