from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_first_enrolled_at_mon = Window.orderBy("first_enrolled_at_month", "grade_status", "grade_status_list_item")
w_row_count_grade_status_list = Window.partitionBy("row_count", "grade_status_list_item").orderBy("distinct_grade_status", "grade_status", "grade_status_list_item")

prepared = (
    df_enrollments
    .filter((F.col("course_id") <= 9) & (F.col("grade_status").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
    )
)

totals = (
    prepared
    .filter(F.col("grade_status_slug").isNotNull())
    .groupBy("grade_status")
    .agg(
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.min("enrolled_at_month").alias("first_enrolled_at_month"),
        F.sum("score").alias("sum_score_check"),
    )
    .filter(F.col("sum_score_check") > 20.2)
    .drop("sum_score_check")
)

flattened = (
    totals
    .select(
        "grade_status",
        "distinct_grade_status",
        "row_count",
        "term_list",
        "first_enrolled_at_month",
        F.explode("grade_status_list").alias("grade_status_list_item"),
    )
)

tagged = (
    flattened
    .select(
        "grade_status",
        "distinct_grade_status",
        "row_count",
        "term_list",
        "first_enrolled_at_month",
        "grade_status_list_item",
        F.coalesce(F.col("distinct_grade_status"), F.lit(0)).alias("distinct_grade_status_filled"),
        F.concat(F.col("grade_status_list_item"), F.lit("#"), F.col("distinct_grade_status").cast("string")).alias("grade_status_list_item_ref"),
        F.date_format(F.col("first_enrolled_at_month"), "yyyy-MM-dd").alias("first_enrolled_at_month_label"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.row_number().over(w_all_by_first_enrolled_at_mon).alias("first_enrolled_at_month_seq"),
        F.lag("first_enrolled_at_month", 3).over(w_row_count_grade_status_list).alias("prev3_first_enrolled_at_month"),
        F.ntile(4).over(w_all_by_first_enrolled_at_mon).alias("first_enrolled_at_month_quartile"),
    )
)

result = (
    windowed
    .select(
        "grade_status",
        "distinct_grade_status",
        "term_list",
        "first_enrolled_at_month",
        "grade_status_list_item",
        "distinct_grade_status_filled",
        "grade_status_list_item_ref",
        "first_enrolled_at_month_seq",
        "prev3_first_enrolled_at_month",
        "first_enrolled_at_month_quartile",
    )
    .orderBy(F.col("distinct_grade_status").desc(), "grade_status_list_item")
)
