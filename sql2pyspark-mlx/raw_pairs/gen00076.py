from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("segment_length").asc_nulls_last())
w2 = Window.partitionBy("city")
w_part = Window.partitionBy("city", "segment")
w4 = Window.partitionBy("city", "segment").orderBy(F.col("segment_length").asc_nulls_last())
w5 = Window.partitionBy("segment", "city")
w_ordered = Window.orderBy(F.col("segment_length").desc_nulls_last())

cleaned = (
    df_customers
    .filter(F.col("country").isin(["TH", "US", "SG"]))
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "is_active",
        F.length("segment").alias("segment_length"),
        F.coalesce("segment", F.lit("OTHER")).alias("segment_clean"),
    )
)

kept_keys = (
    cleaned
    .select("city")
    .subtract(
        df_customers
        .filter(F.col("name").like("Gra%"))
        .select("city")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

scored = verified.withColumn("segment_length_rank", F.rank().over(w))

scored_customers = (
    scored
    .filter(F.col("segment").isin(["SMB", "Consumer"]))
    .select(
        "email",
        "segment",
        "is_active",
        "segment_length",
        "segment_length_rank",
        F.sum("segment_length").over(w2).alias("city_sum_segment_length"),
        F.min("segment_length").over(w_part).alias("city_min_segment_length"),
        F.dense_rank().over(w4).alias("segment_length_dense_rank"),
        F.sum("segment_length").over(w5).alias("segment_sum_segment_length"),
        F.rank().over(w_ordered).alias("segment_length_rank_2"),
    )
)

result = (
    scored_customers
    .groupBy("segment")
    .agg(
        F.countDistinct("segment").alias("distinct_segments"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
        F.percentile_approx("segment_sum_segment_length", [0.25, 0.5, 0.75]).alias("segment_sum_segment_length_quartiles"),
    )
    .orderBy(F.col("smb_events").desc_nulls_last())
)
