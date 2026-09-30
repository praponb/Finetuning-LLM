from pyspark.sql import functions as F

filtered_store_sales = df_store_sales.filter(F.col("units").between(25, 36))

summary = (
    filtered_store_sales
    .groupBy("store_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)

result = summary.select("store_id", "last_sale_date", "units_range")
