from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("city") == "New York") & F.col("sqft").between(5870, 11276) & F.col("sqft").between(5039, 5814))
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("region") == "West", 1).otherwise(0))) / F.count("*")), 1).alias("pct_west"),
    )
    .orderBy(F.col("pct_west").desc(), F.col("opened_month").desc())
)
