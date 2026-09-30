from pyspark.sql import functions as F

src = df_suppliers.filter(F.col("country") == "DE")

agg = (
    src
    .groupBy("city")
    .agg(
        F.max("rating").alias("max_rating"),
        F.count("*").alias("total_count"),
    )
)

result = (
    agg
    .select("city", "max_rating", "total_count")
    .orderBy(F.col("max_rating").desc())
)
