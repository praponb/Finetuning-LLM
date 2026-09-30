from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "DE"]))
    .groupBy("city", "country")
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.count("*").alias("row_count"),
    )
    .orderBy("country")
)
