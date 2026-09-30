from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter((F.col("revenue") >= 1600) & ((F.col("revenue") >= 3100) | F.col("revenue").between(2400, 2900)))
    .withColumn("sale_date_label", F.date_format("sale_date", "yyyy-MM"))
)

summary = (
    src
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        "store_id",
    )
    .agg(
        F.max("units").alias("max_units"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
)

result = summary.filter(F.col("avg_revenue") < 54)
