from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") >= 11) & (F.col("units") < 19))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(F.max("revenue").alias("max_revenue"))
    .orderBy(F.col("max_revenue").desc(), "sale_cohort")
)
