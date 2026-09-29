from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "TH")
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("country") == "DE", F.col("rating")).otherwise(0)).alias("de_rating"),
        F.round(((100 * F.sum(F.when(F.col("country") == "US", 1).otherwise(0))) / F.count("*")), 1).alias("pct_us"),
    )
)
