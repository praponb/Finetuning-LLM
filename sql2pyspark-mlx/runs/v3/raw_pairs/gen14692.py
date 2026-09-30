from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") != 14556) & (F.col("sqft") != 9090) & F.col("opened_date").between("2023-11-15", "2025-10-01"))
    .groupBy("city", "region")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
    .filter(F.col("unique_regions") >= 5)
    .drop("unique_regions")
)
