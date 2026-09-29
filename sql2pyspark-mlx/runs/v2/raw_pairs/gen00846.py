from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["West", "East"]))
    .groupBy("city", F.year("opened_date").alias("opened_year"))
    .agg(
        F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("south_count"),
        F.min("opened_date").alias("min_opened_date"),
    )
    .filter(F.col("south_count") < 31)
    .select("city", "opened_year", "south_count", "min_opened_date")
    .orderBy("city")
)
