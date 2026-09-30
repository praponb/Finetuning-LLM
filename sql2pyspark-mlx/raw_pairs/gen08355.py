from pyspark.sql import functions as F

result = (
    df_suppliers
    .groupBy("country")
    .agg(
        F.min("rating").alias("min_rating"),
        F.round(((100 * F.sum(F.when(F.col("country") == "TH", 1).otherwise(0))) / F.count("*")), 1).alias("pct_th"),
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
    )
    .filter(F.col("sg_count") < 2)
    .drop("sg_count")
)
