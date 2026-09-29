from pyspark.sql import functions as F

valid_store_sales = df_store_sales.filter(F.col("units") != 14)

sale_year_summary = (
    valid_store_sales
    .groupBy(F.year("sale_date").alias("sale_year"), "product_id")
    .agg(
        F.countDistinct("units").alias("unique_products"),
        F.count("*").alias("cnt"),
    )
)

result = (
    sale_year_summary
    .filter(F.col("cnt") >= 7)
    .select("sale_year", "product_id", "cnt")
    .orderBy("sale_year")
)
