from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["Central", "West"]))
    .groupBy("city")
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.min("sqft").alias("min_sqft"),
    )
    .filter(F.col("min_sqft") > 14200)
    .select("city", "min_opened_date", "min_sqft")
    .orderBy(F.col("min_sqft").desc())
)
