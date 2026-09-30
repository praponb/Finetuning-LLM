from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("sqft_tier").orderBy(F.col("distinct_regions").desc())

kept_keys = (
    df_stores
    .select("region", "city")
    .intersect(
        df_stores
        .filter(F.col("sqft") >= 5962)
        .select("region", "city")
    )
)

kept_rows = (
    df_stores
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.region") == F.col("k.region")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("sqft") >= 15579, "PREMIUM").otherwise("STANDARD").alias("sqft_tier"),
        "city",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.max("sqft").alias("max_sqft"),
        F.countDistinct("region").alias("distinct_regions"),
    )
    .withColumn("distinct_regions_rank", F.rank().over(w))
    .orderBy(F.col("num_rows").desc())
)
