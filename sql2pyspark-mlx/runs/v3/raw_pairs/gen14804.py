from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").like("%@startup.io") & (F.col("is_active") | (F.col("segment") != "Corporate")))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "DE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_de"),
    )
)
