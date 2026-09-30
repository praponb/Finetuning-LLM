from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("product_name").like("%Des%") & (F.col("cost") >= 440))
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0))) / F.count("*")), 1).alias("pct_globex"),
    )
)
