from pyspark.sql import functions as F

recent_suppliers = (
    df_suppliers
    .filter((F.col("country") == "US") | F.col("country").isin(["JP", "TH"]))
)

summary = (
    recent_suppliers
    .groupBy("city", "country")
    .agg(
        F.count("*").alias("total_count"),
        F.max("rating").alias("max_rating"),
    )
)

result = (
    summary
    .filter(F.col("max_rating") < 2.8)
    .select("city", "country", "total_count", "max_rating")
    .orderBy("country", F.col("total_count").desc(), "city")
    .limit(1)
)
