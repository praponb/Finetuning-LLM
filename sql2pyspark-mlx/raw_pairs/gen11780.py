from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy(F.col("rating_scaled").asc_nulls_last())
w2 = Window.orderBy(F.col("rating_scaled").desc_nulls_first())
w_part = Window.orderBy(F.col("is_sg").asc_nulls_first())
w4 = Window.orderBy(F.col("is_sg_rounded").desc())

filtered = (
    df_suppliers
    .filter(F.col("country") == "TH")
    .select(
        "*",
        F.round((F.col("rating") * 0.85), 2).alias("rating_scaled"),
        F.when(F.col("country") == "SG", 1).otherwise(0).alias("is_sg"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_clean"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "rating_scaled", "is_sg", "city_clean")
)

remaining_keys = (
    latest
    .select("id")
    .exceptAll(
        df_suppliers
        .filter(F.col("supplier_name").like("%Who%"))
        .select("id")
    )
)

verified = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter(F.col("rating_scaled") < 37)
    .withColumn("is_sg_rounded", F.round("is_sg", 0))
)

scored = (
    cleaned
    .select(
        "id",
        "rating_scaled",
        F.percent_rank().over(w2).alias("rating_scaled_percent_rank"),
        F.cume_dist().over(w_part).alias("is_sg_cume_dist"),
        F.percent_rank().over(w4).alias("is_sg_rounded_percent_rank"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("rating_scaled_percent_rank") >= 0.85, "HIGH").otherwise("LOW").alias("rating_scaled_percent_rank_tier"),
        F.when(F.col("is_sg_rounded_percent_rank") >= 0.78, "LARGE").otherwise("SMALL").alias("is_sg_rounded_percent_rank_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.max("is_sg_cume_dist").alias("max_is_sg_cume_dist"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("rating_scaled_percent_rank"), 2).alias("avg_rating_scaled_percent_rank"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("max_is_sg_cume_dist").asc_nulls_first())
)
