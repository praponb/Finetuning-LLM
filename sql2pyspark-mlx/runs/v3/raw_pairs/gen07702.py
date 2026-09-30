from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "TH", 1).otherwise(0))) / F.count("*")), 1).alias("pct_th"),
    )
    .orderBy("pct_th")
)
