from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") < 24)
    .groupBy(
        F.when(F.col("units") >= 44, "high").when(F.col("units") >= 20, "mid").otherwise("low").alias("units_bucket"),
        F.date_trunc("MONTH", "sale_date").alias("sale_month"),
        "product_id",
    )
    .agg(
        F.sum("units").alias("total_units"),
        F.countDistinct("units").alias("unique_products"),
    )
    .filter(F.col("unique_products") >= 5)
    .drop("unique_products")
    .orderBy("sale_month", F.col("total_units").desc())
)
