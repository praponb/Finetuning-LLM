from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.year("sale_date").alias("sale_year"),
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.sum("units").alias("total_units"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .filter(F.col("avg_units") < 22)
    .orderBy("sale_cohort")
)
