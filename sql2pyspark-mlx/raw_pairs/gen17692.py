from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("rating_tier").orderBy(F.col("uk_rows").desc())

kept_keys = (
    df_suppliers
    .select("id", "city")
    .intersectAll(
        df_suppliers
        .filter(F.col("country").isin(["JP", "UK", "DE"]))
        .select("id", "city")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        F.when(F.col("rating") >= 4.3, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
        "city",
    )
    .agg(
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.count(F.when(F.col("country") == "UK", 1)).alias("uk_rows"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.count("*").alias("event_count"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("rating_stddev") < 1000000)
    .withColumn("uk_rows_rank", F.rank().over(w))
    .orderBy("rating_tier")
)
