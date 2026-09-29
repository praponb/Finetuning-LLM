from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.sum(F.when(F.col("region") == "South", F.col("sqft")).otherwise(0)).alias("south_sqft"),
    )
)
