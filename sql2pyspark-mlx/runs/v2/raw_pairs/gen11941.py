from pyspark.sql import functions as F

result = (
    df_store_sales
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.sum("units").alias("total_units"),
    )
)
