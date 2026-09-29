from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("student_id", "term").orderBy(F.col("score").desc(), F.col("id").desc())

filtered_enrollments = (
    df_enrollments
    .filter(F.col("score") >= 26.2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "yyyy-MM-dd").alias("enrolled_at_label"),
    )
)

latest_per_key = (
    filtered_enrollments
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    latest_per_key
    .groupingSets(
        [["term", "course_id"], ["term"]],
        "term", "course_id",
    )
    .agg(
        F.round(F.avg("score"), 2).alias("avg_score"),
        F.sum(F.when(F.col("term").isNotNull(), F.col("score")).otherwise(0)).alias("score_term_is_not_null"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
    )
    .select(
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        "course_id",
        "avg_score",
        "score_term_is_not_null",
        "term_list",
    )
    .orderBy(F.col("term").asc_nulls_last(), F.col("course_id").asc_nulls_last())
)
