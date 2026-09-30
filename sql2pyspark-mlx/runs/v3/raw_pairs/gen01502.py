from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "TH")
    .groupBy("country")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("age")).otherwise(0)).alias("tokyo_age"),
        F.countDistinct("credit_limit").alias("unique_countrys"),
    )
    .filter(F.col("tokyo_age") > 62)
    .orderBy("unique_countrys", "country")
    .limit(20)
)
