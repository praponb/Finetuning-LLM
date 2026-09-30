from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.min("units").alias("min_units"),
    )
)
