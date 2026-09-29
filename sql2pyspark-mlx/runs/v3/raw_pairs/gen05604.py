from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(~((F.lower(F.trim("supplier_name")) == "global supply") & F.col("supplier_name").like("Tok%")))
    .withColumn("clean_country", F.trim("country"))
)

agg = (
    src
    .groupBy(
        F.when(F.col("rating") >= 4.0, "high").when(F.col("rating") >= 2.5, "mid").otherwise("low").alias("rating_bucket"),
        "country",
    )
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
)

result = (
    agg
    .select("rating_bucket", "avg_rating")
    .orderBy("rating_bucket")
)
