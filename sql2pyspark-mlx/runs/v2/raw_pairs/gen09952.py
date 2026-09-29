from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2]))
    .groupBy("product_id", "store_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.round(F.stddev("revenue"), 2).alias("stddev_revenue"),
    )
    .filter(F.col("stddev_revenue") > 43)
    .orderBy("first_sale_date")
)
