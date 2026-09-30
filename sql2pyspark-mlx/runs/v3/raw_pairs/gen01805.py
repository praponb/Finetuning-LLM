from pyspark.sql import functions as F

recent_store_sales = df_store_sales.filter(F.col("units") < 25)

summary = (
    recent_store_sales
    .groupBy("product_id")
    .agg(
        F.countDistinct("units").alias("unique_stores"),
        F.min("sale_date").alias("first_sale_date"),
    )
)

result = (
    summary
    .filter(F.col("unique_stores") > 7)
    .select("product_id", "unique_stores", "first_sale_date")
)
