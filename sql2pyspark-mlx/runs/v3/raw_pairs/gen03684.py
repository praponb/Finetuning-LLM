from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.year("opened_date") >= 2023) & (F.col("city") == "Singapore"))
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("region") == "North", F.col("sqft")).otherwise(0)).alias("north_sqft"),
    )
    .orderBy("city")
)
