from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") >= 3900) & F.month("sale_date").isin([3, 4, 10]))
    .groupBy(
        "store_id",
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)
