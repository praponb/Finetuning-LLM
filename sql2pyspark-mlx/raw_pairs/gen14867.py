from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("country").orderBy("rating", "id")
w4 = Window.orderBy(F.col("rating").desc_nulls_last())

scored = (
    df_suppliers
    .filter(F.col("rating") < 1.9)
    .select(
        "id",
        "country",
        "rating",
        F.sum("rating").over(w).alias("running_rating"),
        F.ntile(5).over(w2).alias("rating_bucket"),
    )
)

enriched = (
    scored
    .withColumn("next_running_rating", F.lead("running_rating", 1).over(w3))
    .withColumn("rating_rank", F.rank().over(w4))
)

matched_keys = (
    enriched
    .select("id", "country")
    .exceptAll(
        df_suppliers
        .filter(F.col("country").isin(["US", "JP", "SG"]))
        .select("id", "country")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter(F.col("running_rating") != 10)
    .select(
        "*",
        F.round((F.col("rating_bucket") * 1.15), 2).alias("adjusted_rating_bucket"),
        F.col("next_running_rating").cast("string").alias("next_running_rating_str"),
    )
)

grouped = (
    base
    .groupBy(
        "country",
        F.when(F.col("rating_rank") >= 7, "PREMIUM").otherwise("STANDARD").alias("rating_rank_tier"),
    )
    .agg(
        F.round(F.sum("next_running_rating") / F.nullif(F.count(F.when(F.col("country") == "UK", 1)), F.lit(0)), 2).alias("next_running_rating_per_uk"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("country") == "JP").alias("jp_events"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.min("rating").alias("min_rating"),
    )
)

result = (
    grouped
    .select(
        "country",
        "rating_rank_tier",
        "next_running_rating_per_uk",
        "event_count",
        "jp_events",
        "us_count",
        "min_rating",
    )
    .orderBy(F.col("next_running_rating_per_uk").desc())
)
