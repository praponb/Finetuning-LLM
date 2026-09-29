from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("term", "course_id").orderBy(F.col("term_len").desc(), F.col("id").desc())
w_term_by_enrolled_at_prior_6 = Window.partitionBy("term").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-6, -1)
w_term_by_enrolled_at_running = Window.partitionBy("term").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_by_enrolled_at = Window.partitionBy("term").orderBy(F.col("enrolled_at").desc(), "id")

filtered = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

sequenced = (
    latest
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "term_ref",
        "score_filled",
        "term_len",
        F.round(F.avg("term_len").over(w_term_by_enrolled_at_prior_6), 2).alias("term_len_avg_6_prior"),
        F.first("term_len", ignorenulls=True).over(w_term_by_enrolled_at_running).alias("first_known_term_len"),
        F.lag("term_len", 1, 0).over(w_term_by_enrolled_at).alias("prev_term_len"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["course_id", "term_len", "student_id"], ["course_id"]],
        "course_id", "term_len", "student_id",
    )
    .agg(
        F.grouping("course_id").alias("is_total_course_id"),
        F.round(F.avg("term_len"), 2).alias("avg_term_len"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("term_len").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
