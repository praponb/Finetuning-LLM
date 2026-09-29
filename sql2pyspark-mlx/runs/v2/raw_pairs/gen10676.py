from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter(F.month("opened_date").isin([5, 6, 8]) & (F.length("store_name") >= 11))
    .withColumn("opened_date_plus_months", F.add_months("opened_date", 3))
)

opened_plus_months_month_summary = (
    recent_stores
    .groupBy(
        F.date_format("opened_date_plus_months", "yyyy-MM").alias("opened_plus_months_month"),
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
    )
    .agg(
        F.count(F.when(F.col("region") == "South", 1)).alias("south_count"),
        F.sum("sqft").alias("total_sqft"),
    )
)

result = (
    opened_plus_months_month_summary
    .filter(F.col("total_sqft") >= 79000)
    .orderBy("south_count", "opened_month")
)
