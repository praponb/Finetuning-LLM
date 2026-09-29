from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_ids_first_enrolled = Window.partitionBy("distinct_ids", F.to_date("first_enrolled_at"))
w_distinct_ids_by_first_enroll = Window.partitionBy("distinct_ids").orderBy(F.col("first_enrolled_at").desc(), "enrolled_at_ym")
w_distinct_ids_by_first_enroll_2 = Window.partitionBy("distinct_ids").orderBy(F.col("first_enrolled_at").desc(), "enrolled_at_ym").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_ids_by_distinct_ids = Window.partitionBy("distinct_ids").orderBy(F.col("distinct_ids").desc(), "enrolled_at_ym").rowsBetween(-2, Window.currentRow)

valid = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-12 09:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
    )
)

grouped = (
    valid
    .filter(F.col("student_id") <= 13)
    .groupBy(F.date_format("enrolled_at", "yyyy-MM").alias("enrolled_at_ym"))
    .agg(
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

sequenced = (
    grouped
    .select(
        "*",
        F.sum("distinct_ids").over(w_distinct_ids_first_enrolled).alias("distinct_ids_first_enrolled_at_to_date_sum_disti"),
        F.lead("first_enrolled_at").over(w_distinct_ids_by_first_enroll).alias("next_first_enrolled_at"),
        F.lag("distinct_ids").over(w_distinct_ids_by_first_enroll).alias("prev_distinct_ids"),
        F.max("distinct_ids").over(w_distinct_ids_by_first_enroll_2).alias("max_distinct_ids_so_far"),
        F.sum("distinct_ids").over(w_distinct_ids_by_distinct_ids).alias("distinct_ids_sum_3"),
    )
)

result = (
    sequenced
    .select(
        "enrolled_at_ym",
        "distinct_ids_first_enrolled_at_to_date_sum_disti",
        "distinct_ids",
        "distinct_ids_sum_3",
        "prev_distinct_ids",
    )
    .unpivot(
        ids=["enrolled_at_ym"],
        values=["distinct_ids_first_enrolled_at_to_date_sum_disti", "distinct_ids", "distinct_ids_sum_3", "prev_distinct_ids"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
