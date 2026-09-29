from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating_per_de").desc())

result = (
    df_suppliers
    .groupBy(
        "city",
        F.when(F.col("rating") >= 3.8, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.percentile_approx("rating", 0.95).alias("rating_p95"),
        F.round(F.sum("rating") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("rating_per_de"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_rows"),
    )
    .withColumn("rating_per_de_rank", F.rank().over(w))
)
