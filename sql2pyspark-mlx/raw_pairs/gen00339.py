from pyspark.sql import functions as F

src = df_suppliers.filter(F.col("rating") >= 1.8)

suppliers_stats = (
    src
    .groupBy("city")
    .agg(
        F.count("*").alias("num_suppliers"),
        F.max("rating").alias("max_rating"),
    )
)

result = (
    suppliers_stats
    .filter(F.col("max_rating") < 4.0)
    .select("city", "num_suppliers", "max_rating")
    .orderBy(F.col("num_suppliers").desc())
)
