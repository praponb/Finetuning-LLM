from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter((F.col("city") != "New York") & (F.col("city") != "New York"))
    .withColumn("region_title", F.initcap(F.trim("region")))
)

stores_stats = (
    recent_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"), "city")
    .agg(
        F.min("opened_date").alias("first_opened_date"),
        F.max("opened_date").alias("last_opened_date"),
    )
)

result = (
    stores_stats
    .select("opened_quarter", "city", "first_opened_date", "last_opened_date")
)
