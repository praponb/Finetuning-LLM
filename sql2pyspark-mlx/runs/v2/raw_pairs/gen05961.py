from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(~(F.month("opened_date").isin([6, 8, 12]) | (F.col("opened_date") > "2023-11-01")))
    .withColumn("is_new_york", F.when(F.col("city") == "New York", 1).otherwise(0))
)

opened_year_summary = (
    recent_stores
    .groupBy(
        F.year("opened_date").alias("opened_year"),
        F.quarter("opened_date").alias("opened_quarter"),
    )
    .agg(
        F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("south_count"),
        F.min("is_new_york").alias("min_is_new_york"),
    )
)

result = (
    opened_year_summary
    .filter(F.col("south_count") > 42)
    .select("opened_year", "opened_quarter", "south_count", "min_is_new_york")
    .orderBy("min_is_new_york")
)
