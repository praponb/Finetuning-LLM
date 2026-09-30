from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.year("sale_date") == 2024)
    .groupBy(
        "product_id",
        F.when(F.col("units") >= 39, "high").when(F.col("units") >= 20, "mid").otherwise("low").alias("units_bucket"),
    )
    .agg(
        F.countDistinct("units").alias("unique_products"),
        F.max("sale_date").alias("last_sale_date"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") > 1)
    .drop("cnt")
)
