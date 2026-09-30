from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("jp_rating").desc())

kept_keys = (
    df_suppliers
    .select("city")
    .exceptAll(
        df_suppliers
        .filter(F.col("rating") <= 4.1)
        .select("city")
    )
)

kept_rows = (
    df_suppliers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy(
        "country",
        F.when(F.col("rating") >= 3.5, "LARGE").otherwise("SMALL").alias("rating_tier"),
    )
    .agg(
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
    )
    .withColumn("jp_rating_dense_rank", F.dense_rank().over(w))
)
