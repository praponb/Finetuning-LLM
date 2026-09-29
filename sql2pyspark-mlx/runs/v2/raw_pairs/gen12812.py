from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.sum(F.when(F.col("region") == "North", 1).otherwise(0)).alias("north_count"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") < 13100)
    .drop("min_sqft")
    .orderBy("north_count")
)
