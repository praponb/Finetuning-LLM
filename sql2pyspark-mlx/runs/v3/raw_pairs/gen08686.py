from pyspark.sql import functions as F

filtered_store_sales = (
    df_store_sales
    .filter((F.col("units") == 38) & F.col("store_id").isin([2, 4]) & F.col("revenue").isNull())
)

agg = (
    filtered_store_sales
    .groupBy("store_id")
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
)

result = (
    agg
    .filter(F.col("stddev_revenue") > 25)
    .select("store_id", "last_sale_date", "stddev_revenue")
    .orderBy(F.col("stddev_revenue").desc())
)
