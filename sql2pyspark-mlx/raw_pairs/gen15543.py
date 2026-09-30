from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter((F.col("store_id") == 3) & (F.col("sale_date") > "2025-07-13"))
)

agg = (
    src
    .groupBy("product_id")
    .agg(
        F.min("units").alias("min_units"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)

result = (
    agg
    .filter(F.col("min_units") > 19)
    .orderBy(F.col("min_units").desc())
)
