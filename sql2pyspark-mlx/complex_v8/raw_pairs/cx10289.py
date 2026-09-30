from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("dept").orderBy(F.col("course_code_len").desc(), F.col("id").desc())

valid = (
    df_courses
    .filter(~F.col("level").isin("intro", "core"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        F.length(F.trim(F.col("course_code"))).alias("course_code_len"),
    )
)

deduped = (
    valid
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

scored = (
    deduped
    .select(
        "*",
        F.concat_ws("-", F.col("dept"), F.col("course_code")).alias("dept_course_code_label"),
        F.substring(F.trim(F.col("dept")), 1, 2).alias("dept_prefix"),
    )
)

result = (
    scored
    .select(
        "id",
        "course_code",
        "dept",
        "course_code_len",
        "dept_prefix",
        F.when(F.col("credits") >= 4, "large").when(F.col("credits") >= 2, "medium").otherwise("small").alias("credits_tier"),
    )
    .orderBy(F.col("dept").desc())
)
