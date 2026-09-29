from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 4]))
    .groupBy("product_id", "store_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
    .filter(F.col("stddev_units") < 23)
    .select("product_id", "store_id", "latest_sale_date", "stddev_units")
    .orderBy("store_id", F.col("product_id").desc())
)
