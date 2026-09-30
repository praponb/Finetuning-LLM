from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("cohort").orderBy(F.col("gpa_distance").desc(), F.col("id").desc())
w_is_international_by_gpa_ties = Window.partitionBy("is_international").orderBy(F.col("gpa").desc())
w_cohort = Window.partitionBy("cohort")
w_is_international_by_gpa = Window.partitionBy("is_international").orderBy(F.col("gpa").desc(), "id")
w_all_by_stddev_gpa_cume_dist = Window.orderBy("stddev_gpa_cume_dist", "enrolled_on_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_stddev_gpa_cume_dist_2 = Window.orderBy("stddev_gpa_cume_dist", "enrolled_on_month")

cleaned = (
    df_students
    .filter((F.col("gpa") < 3.2) & (F.col("major") == "Biz"))
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "gpa",
        "is_international",
        F.abs(F.col("gpa") - 2.7).alias("gpa_distance"),
    )
)

latest_per_key = (
    cleaned
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_trends = (
    latest_per_key
    .select(
        "*",
        F.round(F.cume_dist().over(w_is_international_by_gpa_ties), 3).alias("gpa_cume_dist"),
        F.round(F.col("gpa_distance") / F.nullif(F.sum("gpa_distance").over(w_cohort), F.lit(0)), 4).alias("gpa_distance_share_of_cohort"),
        (F.col("gpa") - F.lag("gpa").over(w_is_international_by_gpa)).alias("gpa_change"),
    )
)

per_group = (
    with_trends
    .groupBy(F.date_trunc("month", "enrolled_on").alias("enrolled_on_month"))
    .agg(
        F.round(F.stddev("gpa_cume_dist"), 2).alias("stddev_gpa_cume_dist"),
        F.array_sort(F.collect_set("student_name")).alias("student_name_list"),
        F.min("gpa_change").alias("min_gpa_change"),
        F.sum("gpa_change").alias("total_gpa_change"),
        F.sum("gpa_distance_share_of_cohort").alias("total_gpa_distance_share_of_cohort"),
        F.sum("gpa_cume_dist").alias("total_gpa_cume_dist"),
        F.avg("gpa_distance_share_of_cohort").alias("avg_gpa_distance_share_of_cohort_check"),
    )
    .filter(F.col("avg_gpa_distance_share_of_cohort_check") > 0.3)
    .drop("avg_gpa_distance_share_of_cohort_check")
)

with_trends_2 = (
    per_group
    .withColumn("first_known_total_gpa_cume_dist", F.first("total_gpa_cume_dist", ignorenulls=True).over(w_all_by_stddev_gpa_cume_dist))
    .withColumn("stddev_gpa_cume_dist_decile", F.ntile(10).over(w_all_by_stddev_gpa_cume_dist_2))
)

result = (
    with_trends_2
    .select(
        "enrolled_on_month",
        "stddev_gpa_cume_dist",
        "student_name_list",
        "min_gpa_change",
        "total_gpa_change",
        "total_gpa_distance_share_of_cohort",
        "total_gpa_cume_dist",
        "first_known_total_gpa_cume_dist",
        "stddev_gpa_cume_dist_decile",
    )
    .orderBy(F.col("min_gpa_change").desc())
)
