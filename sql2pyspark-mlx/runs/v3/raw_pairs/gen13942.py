from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 5)
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(F.min("sale_date").alias("first_sale_date"))
    .orderBy("sale_cohort")
)
