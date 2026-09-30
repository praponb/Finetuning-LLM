from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("level").orderBy(F.col("credits_distance").desc(), F.col("id").desc())

valid_courses = (
    df_courses
    .filter((F.upper(F.trim(F.col("course_code"))).like("C%")) & (F.col("credits") >= 2))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.abs(F.col("credits") - 3).alias("credits_distance"),
    )
)

enriched = (
    valid_courses
    .filter(F.col("credits_distance") <= 3)
    .select(
        "id",
        "level",
        "credits_distance",
        F.abs(F.col("credits_distance") - 2).alias("credits_distance_distance"),
        F.when(F.col("credits") >= 3, "large").when(F.col("credits") >= 2, "medium").otherwise("small").alias("credits_tier"),
        F.initcap(F.trim(F.col("level"))).alias("level_clean"),
    )
)

latest = (
    enriched
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest
    .rollup("credits_distance", "credits_distance_distance")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.stddev("credits_distance"), 2).alias("stddev_credits_distance"),
    )
    .orderBy(
        F.col("credits_distance").asc_nulls_last(),
        F.col("credits_distance_distance").asc_nulls_last(),
    )
)
