from pyspark.sql import functions as F

src = (
    df_stores
    .filter((F.col("opened_date") <= "2025-08-01") | (F.upper(F.trim("store_name")) == "SIAM FLAGSHIP"))
)

region_summary = (
    src
    .groupBy("region", F.month("opened_date").alias("opened_month"))
    .agg(F.round(F.stddev("sqft"), 2).alias("stddev_sqft"))
)

result = region_summary.orderBy(F.col("stddev_sqft").desc())
