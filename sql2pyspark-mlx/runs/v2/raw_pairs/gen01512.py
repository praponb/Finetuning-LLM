from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 7]))
    .withColumn("sale_date_label", F.date_format("sale_date", "yyyy-MM-dd"))
)

store_id_summary = (
    filtered_store_sales
    .groupBy("store_id")
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)

result = store_id_summary
