from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(15, 23) & ((F.col("store_id") == 5) | F.col("units").isNotNull()))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        "store_id",
    )
    .agg(F.count("units").alias("units_count"))
    .filter(F.col("units_count") >= 4)
)
