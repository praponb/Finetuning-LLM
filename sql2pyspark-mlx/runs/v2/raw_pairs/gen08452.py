from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.sum("units").alias("total_units"),
    )
    .filter(F.col("total_units") > 130)
    .orderBy("sale_quarter")
)
