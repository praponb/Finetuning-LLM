from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("launch_date") >= "2024-08-19")
    .groupBy(F.year("launch_date").alias("launch_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("category") == "Electronics", 1).otherwise(0))) / F.count("*")), 1).alias("pct_electronics"),
    )
)
