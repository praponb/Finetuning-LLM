from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_quarter = Window.partitionBy("enrolled_at_quarter")
w_term_by_enrolled_at = Window.partitionBy("term").orderBy(F.col("enrolled_at").desc(), "id")
w_enrolled_at_to_date = Window.partitionBy(F.to_date("enrolled_at"))
w_enrolled_at_to_date_by_enrol = Window.partitionBy(F.to_date("enrolled_at")).orderBy(F.col("enrolled_at_quarter_share_of_enrolled_at_quarter").desc(), "id")
w_enrolled_at_to_date_by_enrol_2 = Window.partitionBy(F.to_date("enrolled_at")).orderBy(F.col("enrolled_at_quarter_share_of_enrolled_at_quarter").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared_enrollments = (
    df_enrollments
    .filter((F.col("term") == "2024T2") & (F.col("grade_status").isin("INCOMPLETE", "FAIL")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.quarter(F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

metrics = (
    prepared_enrollments
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "term_slug",
        F.round(F.col("enrolled_at_quarter") / F.nullif(F.sum("enrolled_at_quarter").over(w_enrolled_at_quarter), F.lit(0)), 4).alias("enrolled_at_quarter_share_of_enrolled_at_quarter"),
        F.count(F.lit(1)).over(w_enrolled_at_quarter).alias("rows_in_enrolled_at_quarter"),
        F.lead("enrolled_at_quarter").over(w_term_by_enrolled_at).alias("next_enrolled_at_quarter"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.max("rows_in_enrolled_at_quarter").over(w_enrolled_at_to_date).alias("enrolled_at_to_date_max_rows_in_enrolled_at_quar"),
        (F.col("next_enrolled_at_quarter") - F.lag("next_enrolled_at_quarter").over(w_enrolled_at_to_date_by_enrol)).alias("next_enrolled_at_quarter_change"),
        F.first("next_enrolled_at_quarter").over(w_enrolled_at_to_date_by_enrol_2).alias("first_next_enrolled_at_quarter"),
        F.round(F.percent_rank().over(w_enrolled_at_to_date_by_enrol), 3).alias("enrolled_at_quarter_share_of_enrolled_at_quarter_2"),
        F.min("next_enrolled_at_quarter").over(w_enrolled_at_to_date).alias("enrolled_at_to_date_min_next_enrolled_at_quarter"),
        F.nth_value("rows_in_enrolled_at_quarter", 2).over(w_enrolled_at_to_date_by_enrol_2).alias("second_rows_in_enrolled_at_quarter"),
    )
)

derived = (
    ranked
    .withColumn("next_enrolled_at_quarter_filled", F.coalesce(F.col("next_enrolled_at_quarter"), F.lit(0)))
)

result = (
    derived
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "term_slug",
        "rows_in_enrolled_at_quarter",
        "next_enrolled_at_quarter",
        "enrolled_at_to_date_max_rows_in_enrolled_at_quar",
        "first_next_enrolled_at_quarter",
        "enrolled_at_quarter_share_of_enrolled_at_quarter_2",
        "enrolled_at_to_date_min_next_enrolled_at_quarter",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.lower(F.trim(F.col("term_slug"))).alias("term_slug_clean"),
    )
    .orderBy(F.col("term").asc(), "id")
    .limit(10)
)
