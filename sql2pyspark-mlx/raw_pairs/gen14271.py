from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0))) / F.count("*")), 1).alias("pct_acme"),
        F.max("launch_date").alias("last_launch_date"),
    )
    .filter(F.col("pct_acme") >= 79)
    .orderBy("pct_acme")
)
