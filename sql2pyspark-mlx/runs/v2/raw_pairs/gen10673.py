from pyspark.sql import functions as F

filtered_stores = df_stores.filter(F.col("region") == "North")

opened_month_summary = (
    filtered_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "Central", 1).otherwise(0))) / F.count("*")), 1).alias("pct_central"),
    )
)

result = (
    opened_month_summary
    .filter(F.col("pct_central") >= 66)
    .orderBy("opened_month")
)
