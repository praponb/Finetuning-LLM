from pyspark.sql import functions as F

filtered_suppliers = df_suppliers.filter(F.col("city").isin(["Singapore", "Tokyo", "Berlin"]))

agg = (
    filtered_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.6, "high").when(F.col("rating") >= 1.7, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "UK", F.col("rating")).otherwise(0)).alias("uk_rating"),
        F.sum(F.when(F.col("country") == "DE", F.col("rating")).otherwise(0)).alias("de_rating"),
    )
)

result = (
    agg
    .filter(F.col("uk_rating") > 41)
    .orderBy("rating_bucket")
)
