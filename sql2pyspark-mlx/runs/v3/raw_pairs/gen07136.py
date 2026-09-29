from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("product_id") == 10) & (F.col("store_id") == 1))
    .groupBy(
        "product_id",
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
    .orderBy("product_id", "last_sale_date")
)
