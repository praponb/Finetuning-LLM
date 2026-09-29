from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("units") >= 21)

sale_cohort_summary = (
    src
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(F.max("sale_date").alias("last_sale_date"))
)

result = (
    sale_cohort_summary
    .select("sale_cohort", "last_sale_date")
    .orderBy("last_sale_date", "sale_cohort")
    .limit(1)
)
