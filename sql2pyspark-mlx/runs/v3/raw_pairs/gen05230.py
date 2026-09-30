from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(5300, 6400))
    .groupBy("country", "segment", "city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
    )
)
