from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_enrolled_at_ru = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_student_id_by_enrolled_at_mo = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(-1, Window.currentRow)
w_student_id_by_enrolled_at_mo_2 = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_student_id = Window.partitionBy("student_id")

joined_ranges = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.score").alias("prior_score_12h"),
    )
)

metrics = (
    joined_ranges
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_rows_12h",
        "prior_score_12h",
        F.last("enrolled_at", ignorenulls=True).over(w_student_id_by_enrolled_at_ru).alias("last_known_enrolled_at"),
        F.max("prior_rows_12h").over(w_student_id_by_enrolled_at_mo).alias("prior_rows_12h_max_2"),
        F.round(F.avg("prior_score_12h").over(w_student_id_by_enrolled_at_mo_2), 2).alias("prior_score_12h_avg_7"),
        F.min("prior_score_12h").over(w_student_id).alias("student_id_min_prior_score_12h"),
    )
)

student_id_stats = (
    metrics
    .groupBy("student_id")
    .agg(F.max("prior_rows_12h_max_2").alias("max_prior_rows_12h_max_2"))
)

compared = (
    metrics.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.prior_rows_12h_max_2") == F.col("g.max_prior_rows_12h_max_2"))
    .select(
        "s.*",
        "g.max_prior_rows_12h_max_2",
        F.round(F.col("s.prior_rows_12h_max_2") - F.col("g.max_prior_rows_12h_max_2"), 2).alias("prior_rows_12h_max_2_minus_max"),
    )
)

tagged = (
    compared
    .filter(F.col("prior_rows_12h_max_2") < 16)
    .withColumn("days_since_enrolled_at", F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")))
)

result = (
    tagged
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_rows_12h",
        "prior_score_12h",
        "student_id_min_prior_score_12h",
        "max_prior_rows_12h_max_2",
        "prior_rows_12h_max_2_minus_max",
        "days_since_enrolled_at",
    )
)
