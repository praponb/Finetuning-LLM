from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city").orderBy(F.col("is_singapore").desc_nulls_last())
w4 = Window.partitionBy("segment").orderBy(F.col("is_singapore_minus_is_singapore_cume_dist").desc())
w5 = Window.orderBy(F.col("is_singapore").desc())
w6 = Window.partitionBy("segment").orderBy(F.col("is_singapore").asc_nulls_first())

deduped = (
    df_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "city", "segment", "signup_date", "is_active")
)

remaining_keys = (
    deduped
    .select("id", "segment")
    .intersect(
        df_customers
        .filter(F.col("country") == "UK")
        .select("id", "segment")
    )
)

verified = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("city").isNull())
    .withColumn("is_singapore", F.when(F.col("city") == "Singapore", 1).otherwise(0))
)

scored = (
    base
    .filter(F.col("signup_date") <= "2023-10-01")
    .select(
        "id",
        "segment",
        "is_active",
        "is_singapore",
        F.min("is_singapore").over(w2).alias("city_min_is_singapore"),
        F.cume_dist().over(w3).alias("is_singapore_cume_dist"),
    )
)

filtered = (
    scored
    .filter((F.col("segment").eqNullSafe(F.lit("Consumer"))) & (F.col("is_singapore") >= 52))
    .select(
        "id",
        "segment",
        "is_active",
        "is_singapore",
        "city_min_is_singapore",
        "is_singapore_cume_dist",
        F.floor("is_singapore_cume_dist").alias("is_singapore_cume_dist_whole"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_filled"),
    )
)

base_customers = (
    filtered
    .filter(F.col("city_min_is_singapore") <= 31)
    .select(
        "segment",
        "is_active",
        "is_singapore",
        "city_min_is_singapore",
        "is_singapore_cume_dist_whole",
        "segment_filled",
        (F.col("is_singapore") - F.col("is_singapore_cume_dist")).alias("is_singapore_minus_is_singapore_cume_dist"),
    )
)

metrics = (
    base_customers
    .withColumn("is_singapore_minus_is_singapore_cume_dist_dense_rank", F.dense_rank().over(w4))
    .withColumn("is_singapore_rank", F.rank().over(w5))
    .withColumn("is_singapore_cume_dist", F.cume_dist().over(w6))
)

grouped = (
    metrics
    .groupBy("segment")
    .agg(
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_rows"),
        F.round(F.sum("is_singapore_minus_is_singapore_cume_dist_dense_rank") / F.nullif(F.count(F.when(F.col("segment") == "Corporate", 1)), F.lit(0)), 2).alias("is_singapore_minus_is_singapore_cume_dist_dense_rank_per_corporate"),
    )
)

result = (
    grouped
    .select(
        "segment",
        "corporate_rows",
        "is_singapore_minus_is_singapore_cume_dist_dense_rank_per_corporate",
        F.when(F.col("segment").rlike("^(SMB|Consumer)$"), "EXTENDED").when(F.col("segment").rlike("^(Corporate)$"), "OTHER_ZONE").otherwise("CORE").alias("segment_zone"),
    )
)
