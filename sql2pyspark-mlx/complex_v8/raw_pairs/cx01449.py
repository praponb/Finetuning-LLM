from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_n_rows = Window.partitionBy("n_rows")
w_n_rows_distinct_ids_by_n_row = Window.partitionBy("n_rows", "distinct_ids").orderBy(F.col("n_rows").desc(), "is_international", "enrolled_on_ym")

prepared_students = (
    df_students
    .filter((F.col("student_name").isNotNull()) & (F.col("enrolled_on") >= F.lit("2024-10-09").cast("date")))
    .select(
        "id",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

stats = (
    prepared_students
    .groupBy(
        "is_international",
        F.date_format("enrolled_on", "yyyy-MM").alias("enrolled_on_ym"),
    )
    .agg(
        F.round(F.avg("gpa"), 2).alias("avg_gpa"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

windowed = (
    stats
    .select(
        "is_international",
        "enrolled_on_ym",
        "distinct_ids",
        "n_rows",
        F.sum("avg_gpa").over(w_n_rows).alias("n_rows_sum_avg_gpa"),
        F.rank().over(w_n_rows_distinct_ids_by_n_row).alias("n_rows_rank"),
    )
)

leaders = (
    windowed
    .filter(F.col("n_rows_rank") <= 2)
    .drop("n_rows_rank")
)

result = (
    leaders
    .groupingSets(
        [["n_rows", "distinct_ids"], ["n_rows"], ["distinct_ids"]],
        "n_rows", "distinct_ids",
    )
    .agg(
        F.percentile_approx("n_rows_sum_avg_gpa", 0.5).alias("median_n_rows_sum_avg_gpa"),
        F.max("n_rows_sum_avg_gpa").alias("max_n_rows_sum_avg_gpa"),
    )
    .orderBy(
        F.col("n_rows").asc_nulls_last(),
        F.col("distinct_ids").asc_nulls_last(),
    )
)
