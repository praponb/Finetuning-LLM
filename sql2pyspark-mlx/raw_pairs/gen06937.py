from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 2.6) & (F.col("city") == "London"))
    .groupBy("country")
    .agg(
        F.min("rating").alias("min_rating"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
)
