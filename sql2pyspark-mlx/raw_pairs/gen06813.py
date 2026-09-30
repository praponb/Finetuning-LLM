from pyspark.sql import functions as F

base = df_suppliers.filter(F.col("city").isin(["New York", "London", "Singapore"]))

suppliers_stats = (
    base
    .groupBy("country")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "US", 1).otherwise(0))) / F.count("*")), 1).alias("pct_us"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
)

result = suppliers_stats.select("country", "pct_us")
