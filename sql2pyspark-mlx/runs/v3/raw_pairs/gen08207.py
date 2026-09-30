from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "store_id",
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        "product_id",
    )
    .agg(F.min("sale_date").alias("min_sale_date"))
    .orderBy(F.col("store_id").desc(), F.col("product_id").desc())
)
