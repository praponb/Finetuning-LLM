from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") < 3.0) & (F.col("city") == "Singapore") & (F.col("rating") > 4.1))
    .cube("city", "country")
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.sum("rating").alias("total_rating"),
    )
)
