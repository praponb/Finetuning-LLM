from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("dept").orderBy(F.col("credits").desc(), F.col("id").desc())

base = (
    df_courses
    .filter((F.col("dept") == "Biz") | (F.length(F.trim(F.col("course_code"))) >= 5))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
    )
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

labeled = (
    latest
    .select(
        "id",
        "credits",
        F.concat_ws(": ", F.col("dept"), F.col("course_code")).alias("dept_course_code_label"),
        F.coalesce(F.col("dept"), F.lit("UNKNOWN")).alias("dept_or_unknown"),
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
    )
)

result = (
    labeled
    .select(
        "id",
        "credits",
        "dept_course_code_label",
        "dept_or_unknown",
        "credits_filled",
        F.length(F.trim(F.col("dept_or_unknown"))).alias("dept_or_unknown_len"),
        F.greatest(F.col("credits"), F.col("credits_filled")).alias("greatest_credits_credits_filled"),
    )
    .orderBy(
        F.col("credits_filled").desc(),
        F.col("greatest_credits_credits_filled").desc(),
    )
)
