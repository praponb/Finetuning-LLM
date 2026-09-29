from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.col("units") >= 27)

store_id_summary = (
    filtered_store_sales
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_stores"),
        F.min("sale_date").alias("min_sale_date"),
    )
)

result = (
    store_id_summary
    .filter(F.col("unique_stores") >= 4)
    .select("store_id", "unique_stores", "min_sale_date")
)
