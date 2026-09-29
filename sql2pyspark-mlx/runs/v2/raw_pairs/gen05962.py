from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id", F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.min("sale_date").alias("min_sale_date"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .filter(F.col("stddev_units") < 56)
    .orderBy("sale_quarter", "min_sale_date")
)
