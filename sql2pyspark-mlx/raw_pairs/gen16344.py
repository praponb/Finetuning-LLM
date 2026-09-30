from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") <= "2023-04-09") | (F.col("sqft") != 10265))
    .groupBy("region")
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
        F.sum("sqft").alias("total_sqft"),
    )
    .filter(F.col("new_york_sqft") < 47)
)
