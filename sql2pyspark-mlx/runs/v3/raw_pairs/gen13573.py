from pyspark.sql import functions as F

src = (
    df_customers
    .filter((F.col("country") == "US") & (F.col("credit_limit").between(6200, 11100) | F.col("name").like("%Alic%")))
)

city_summary = (
    src
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
    )
)

result = city_summary.select("city", "pct_singapore")
