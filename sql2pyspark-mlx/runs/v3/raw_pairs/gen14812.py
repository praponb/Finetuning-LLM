from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") <= "2023-07-02") | F.month("opened_date").isin([1, 6, 11]))
    .groupBy("city")
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .orderBy(F.col("min_opened_date").desc())
)
