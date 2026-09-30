from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("city") != "New York") & F.month("opened_date").isin([3, 9, 12]) & (F.col("sqft") == 6577))
)

city_summary = (
    base
    .groupBy("city", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
)

result = city_summary.select("city", "opened_month", "sqft_range")
