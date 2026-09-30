from pyspark.sql import functions as F

filtered_suppliers = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "UK", "TH"]) & (F.col("rating") > 3.4))
    .withColumn("clean_city", F.trim("city"))
)

country_summary = (
    filtered_suppliers
    .groupBy("country")
    .agg(
        F.round(F.avg("rating"), 2).alias("avg_rating"),
        F.count("*").alias("total_count"),
    )
)

result = (
    country_summary
    .filter(F.col("total_count") < 7)
    .select("country", "total_count")
)
