from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(F.col("product_id") == 9)
    .withColumn("sale_date_label", F.date_format("sale_date", "MMM yyyy"))
)

sale_month_summary = (
    recent_store_sales
    .groupBy(F.month("sale_date").alias("sale_month"), "store_id")
    .agg(F.countDistinct("units").alias("unique_products"))
)

result = (
    sale_month_summary
    .select("sale_month", "store_id", "unique_products")
    .orderBy("store_id")
)
