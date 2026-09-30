from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(
        F.count("units").alias("units_count"),
        F.max("sale_date").alias("last_sale_date"),
    )
)
