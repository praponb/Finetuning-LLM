from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_enrolled_on = Window.orderBy(F.col("enrolled_on").desc(), "id")
w_is_international = Window.partitionBy("is_international")
w_all_by_enrolled_on_full = Window.orderBy(F.col("enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_students
    .filter((F.col("gpa") > 2.7) & (~F.col("is_international")))
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

qualified = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.grade_status").isin("WITHDRAWN", "FAIL", "PASS")), "left_anti")
)

metrics = (
    qualified
    .select(
        "*",
        F.lag("enrolled_on", 2).over(w_all_by_enrolled_on).alias("prev2_enrolled_on"),
        F.round(F.col("gpa") / F.nullif(F.sum("gpa").over(w_is_international), F.lit(0)), 4).alias("gpa_share_of_is_international"),
        F.nth_value("enrolled_on", 2).over(w_all_by_enrolled_on_full).alias("second_enrolled_on"),
    )
)

result = (
    metrics
    .groupBy("second_enrolled_on", "prev2_enrolled_on")
    .pivot("major", ["CS", "Math", "Biz", "Bio"])
    .agg(F.max("gpa_share_of_is_international"))
    .orderBy("second_enrolled_on", "prev2_enrolled_on")
)
