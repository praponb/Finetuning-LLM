from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dept_by_credits = Window.partitionBy("dept").orderBy(F.col("credits").desc(), "id")
w_dept = Window.partitionBy("dept")

cleaned = (
    df_courses
    .filter((F.col("dept") == "Bio") & (F.lower(F.col("course_code")).like("%cs%")))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.initcap(F.trim(F.col("course_code"))).alias("course_code_clean"),
        F.concat_ws("-", F.col("dept"), F.col("course_code")).alias("dept_course_code_label"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.lead("credits").over(w_dept_by_credits).alias("next_credits"),
        F.round(F.col("credits") - F.avg("credits").over(w_dept), 2).alias("credits_vs_dept_avg"),
        F.count(F.lit(1)).over(w_dept).alias("rows_in_dept"),
        F.round(F.col("credits") / F.nullif(F.sum("credits").over(w_dept), F.lit(0)), 4).alias("credits_share_of_dept"),
    )
)

qualified = (
    sequenced.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.student_id") >= 6) & (F.hour(F.col("e.enrolled_at")).between(7, 13)), "left_anti")
    .filter(F.col("credits") > 2)
)

result = (
    qualified
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        "course_code_clean",
        "dept_course_code_label",
        "next_credits",
        "credits_vs_dept_avg",
        "credits_share_of_dept",
    )
    .orderBy(F.col("next_credits").desc(), F.col("id").asc())
)
