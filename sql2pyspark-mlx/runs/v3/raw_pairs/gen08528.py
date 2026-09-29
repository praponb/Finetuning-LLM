from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 4, 5]) & (F.col("units") <= 26))
    .withColumn("units_band", F.when(F.col("units") >= 45, "High").when(F.col("units") >= 13, "Medium").otherwise("Low"))
)

agg = (
    filtered_store_sales
    .groupBy(F.date_format("sale_date", "yyyy-MM").alias("sale_month"), "store_id")
    .agg(
        F.avg("units").alias("avg_units"),
        F.sum("revenue").alias("total_revenue"),
    )
)

result = (
    agg
    .filter(F.col("avg_units") > 27)
    .orderBy(F.col("avg_units").desc(), "sale_month", "store_id")
    .limit(50)
)
