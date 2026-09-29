from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 6, 9, 10]) | (F.col("sale_date") >= "2023-12-15"))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        F.date_format("sale_date", "yyyy-MM").alias("sale_month"),
    )
    .agg(F.max("sale_date").alias("last_sale_date"))
    .orderBy("sale_month", "sale_cohort")
    .limit(1)
)
