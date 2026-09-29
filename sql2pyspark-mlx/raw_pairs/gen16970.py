from pyspark.sql import functions as F

src = df_suppliers.filter(F.col("country") == "DE")

agg = (
    src
    .groupBy(
        "country",
        F.when(F.col("rating") >= 4.1, "high").when(F.col("rating") >= 1.5, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.count("rating").alias("rating_count"))
)

result = (
    agg
    .orderBy("country", "rating_bucket")
    .limit(10)
)
