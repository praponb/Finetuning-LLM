from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "Singapore") & (F.col("country") == "US") & F.col("rating").between(2.3, 3.4))
    .groupBy("country")
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .orderBy("stddev_rating", F.col("avg_rating").desc())
)
