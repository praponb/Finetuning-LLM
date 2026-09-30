from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_credits = Window.partitionBy("credits")
w_all_by_credits_ties = Window.orderBy(F.col("credits").desc())
w_all_by_credits = Window.orderBy(F.col("credits").desc(), "id")
w_all_by_credits_full = Window.orderBy(F.col("credits").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_courses
    .filter(~F.col("level").isin("intro", "advanced"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        F.concat_ws("|", F.col("level"), F.col("dept")).alias("level_dept_label"),
    )
)

with_activity = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left_semi")
)

sequenced = (
    with_activity
    .select(
        "*",
        F.min("credits").over(w_credits).alias("credits_min_credits"),
        F.round(F.percent_rank().over(w_all_by_credits_ties), 3).alias("credits_percent_rank"),
        F.round(F.col("credits") - F.avg("credits").over(w_credits), 2).alias("credits_vs_credits_avg"),
        F.lead("credits", 2).over(w_all_by_credits).alias("next2_credits"),
        F.round(F.col("credits") / F.nullif(F.sum("credits").over(w_credits), F.lit(0)), 4).alias("credits_share_of_credits"),
        F.last("credits").over(w_all_by_credits_full).alias("last_credits"),
    )
)

tagged = (
    sequenced
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level_dept_label",
        "credits_min_credits",
        "credits_percent_rank",
        "credits_vs_credits_avg",
        "next2_credits",
        F.greatest(F.col("credits_share_of_credits"), F.col("last_credits")).alias("greatest_credits_share_of_credits_last_credits"),
        (F.floor(F.col("credits_min_credits") / 5) * 5).alias("credits_min_credits_bucket"),
        F.concat(F.col("course_code"), F.lit("#"), F.col("credits").cast("string")).alias("course_code_ref"),
    )
)

result = (
    tagged
    .groupingSets(
        [["next2_credits", "dept"], ["next2_credits"], ["dept"]],
        "next2_credits", "dept",
    )
    .agg(
        F.max("next2_credits").alias("max_next2_credits"),
        F.array_sort(F.collect_set("course_code")).alias("course_code_list"),
    )
    .orderBy(F.col("next2_credits").asc_nulls_last(), F.col("dept").asc_nulls_last())
)
