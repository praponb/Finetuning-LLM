from pyspark.sql import functions as F

src = df_store_sales.filter(F.col("product_id") == 1)

agg = (
    src
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(F.countDistinct("units").alias("unique_stores"))
)

result = (
    agg
    .select("sale_cohort", "unique_stores")
    .orderBy(F.col("unique_stores").desc())
)
