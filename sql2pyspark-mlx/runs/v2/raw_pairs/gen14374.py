from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter(F.col("country").isin(["JP", "DE"]) & (F.col("city").isin(["New York", "Berlin", "Bangkok"]) | F.col("supplier_name").like("Gl%")))
)

agg = (
    src
    .groupBy(
        F.when(F.col("rating") >= 3.9, "high").when(F.col("rating") >= 2.0, "mid").otherwise("low").alias("rating_bucket"),
    )
    .agg(F.max("rating").alias("max_rating"))
)

result = (
    agg
    .select("rating_bucket", "max_rating")
    .orderBy("rating_bucket", F.col("max_rating").desc())
    .limit(1)
)
