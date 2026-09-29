from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") <= 38) & (F.col("sale_date") < "2023-12-01"))
    .groupBy("store_id")
    .agg(
        F.min("units").alias("min_units"),
        F.min("sale_date").alias("min_sale_date"),
    )
    .orderBy("min_units", "store_id")
    .limit(50)
)
