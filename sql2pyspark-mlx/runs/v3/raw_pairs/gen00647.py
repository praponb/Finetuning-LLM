from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("name").like("O%") & F.col("segment").isNotNull()) | F.col("age").isNotNull())
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
        F.countDistinct("age").alias("unique_countrys"),
    )
    .filter(F.col("unique_countrys") < 3)
)
