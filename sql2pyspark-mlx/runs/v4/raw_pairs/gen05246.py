from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        "store_id",
    )
    .agg(
        F.avg("revenue").alias("avg_revenue"),
        F.sum("units").alias("total_units"),
    )
    .filter(F.col("total_units") > 70)
    .drop("total_units")
)
