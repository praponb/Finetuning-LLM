from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(F.max("units").alias("max_units"))
    .filter(F.col("max_units") >= 39)
    .orderBy(F.col("max_units").desc())
)
