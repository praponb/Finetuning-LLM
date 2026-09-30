from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") <= 10686) | (F.col("opened_date") <= "2025-05-01"))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "city")
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
)
