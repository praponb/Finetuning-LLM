from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("credits_tier", "dept").orderBy("credits", "id")
w_credits_by_credits = Window.partitionBy("credits").orderBy(F.col("credits").desc_nulls_first(), "id")
w_credits_level = Window.partitionBy("credits", "level")
w_dept_by_credits = Window.partitionBy("dept").orderBy(F.col("credits").desc(), "id")
w_dedup = Window.partitionBy("level", "dept").orderBy(F.col("credits").desc(), F.col("id").desc())
w_all_by_credits_level_min_cre = Window.orderBy("credits_level_min_credits", "id")
w_dept_by_credits_dense_rank = Window.partitionBy("dept").orderBy(F.col("credits_dense_rank").desc(), "id")

recent = (
    df_courses
    .filter((F.col("dept") == "Art") & (F.col("credits") >= 2))
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.when(F.col("credits") >= 3, "large").when(F.col("credits") >= 2, "medium").otherwise("small").alias("credits_tier"),
        F.concat_ws("-", F.col("level"), F.col("dept")).alias("level_dept_label"),
        F.lower(F.regexp_replace(F.trim(F.col("level")), " ", "_")).alias("level_slug"),
    )
)

latest = (
    recent
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

metrics = (
    latest
    .select(
        "*",
        F.dense_rank().over(w_credits_by_credits).alias("credits_dense_rank"),
        F.sum("credits").over(w_credits_level).alias("credits_level_sum_credits"),
        F.min("credits").over(w_credits_level).alias("credits_level_min_credits"),
        F.lag("credits", 3).over(w_dept_by_credits).alias("prev3_credits"),
    )
)

unique_rows = (
    metrics
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

ranked = (
    unique_rows
    .select(
        "*",
        F.ntile(4).over(w_all_by_credits_level_min_cre).alias("credits_level_min_credits_quartile"),
        F.dense_rank().over(w_dept_by_credits_dense_rank).alias("credits_dense_rank_dense_rank"),
        F.round(F.cume_dist().over(w_all_by_credits_level_min_cre), 3).alias("credits_level_min_credits_cume_dist"),
    )
)

dept_stats = (
    ranked
    .groupBy("dept")
    .agg(F.max("credits_level_min_credits_quartile").alias("max_credits_level_min_credits_quartile"))
)

above_average = (
    ranked.alias("s")
    .join(dept_stats.alias("g"), F.col("g.dept") == F.col("s.dept"), "inner")
    .filter(F.col("s.credits_level_min_credits_quartile") == F.col("g.max_credits_level_min_credits_quartile"))
    .select(
        "s.*",
        "g.max_credits_level_min_credits_quartile",
        F.round(F.col("s.credits_level_min_credits_quartile") - F.col("g.max_credits_level_min_credits_quartile"), 2).alias("credits_level_min_credits_quartile_minus_max"),
    )
)

result = (
    above_average
    .groupBy("credits_level_min_credits_quartile", "credits_dense_rank")
    .pivot("level", ["intro", "core"])
    .agg(F.max("credits_level_min_credits"))
)
