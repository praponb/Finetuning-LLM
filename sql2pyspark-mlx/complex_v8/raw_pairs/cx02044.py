from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_international_row_count = Window.partitionBy("is_international", "row_count")
w_row_count_distance_is_intern = Window.partitionBy("row_count_distance", "is_international").orderBy("row_count_distance", "is_international").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_row_count_distance_is_intern_2 = Window.partitionBy("row_count_distance", "is_international").orderBy("row_count_distance", "is_international").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_row_count = Window.partitionBy("row_count")
w_is_international_by_row_coun = Window.partitionBy("is_international").orderBy("row_count_distance", "is_international")
w_is_international_by_row_coun_2 = Window.partitionBy("is_international").orderBy("row_count_distance", "is_international").rowsBetween(-6, Window.currentRow)
w_rn_by_row_count_distance_max = Window.partitionBy("rn").orderBy(F.col("row_count_distance_max_7").desc(), "is_international")
w_is_international_by_is_inter = Window.partitionBy("is_international").orderBy(F.col("is_international_row_count_avg_row_count_distanc").asc_nulls_last(), "is_international").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_row_count_distance_rank_row = Window.partitionBy("row_count_distance_rank", "row_count_distance")
w_rn_by_row_count_distance_max_2 = Window.partitionBy("rn").orderBy(F.col("row_count_distance_max_7").desc())

valid_students = (
    df_students
    .filter(F.upper(F.trim(F.col("student_name"))).like("G%"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.concat(F.col("major"), F.lit("#"), F.col("id").cast("string")).alias("major_ref"),
    )
)

per_group = (
    valid_students
    .filter(F.col("major_ref").isNotNull())
    .groupBy("is_international")
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("is_international"), F.col("gpa"))).alias("gpa_is_international_true"),
    )
)

enriched = per_group.select("*", F.abs(F.col("row_count") - 8).alias("row_count_distance"))

sequenced = (
    enriched
    .select(
        "is_international",
        "row_count",
        "gpa_is_international_true",
        "row_count_distance",
        F.round(F.avg("row_count_distance").over(w_is_international_row_count), 2).alias("is_international_row_count_avg_row_count_distanc"),
        F.last("gpa_is_international_true").over(w_row_count_distance_is_intern).alias("last_gpa_is_international_true"),
        F.max("row_count_distance").over(w_row_count_distance_is_intern_2).alias("max_row_count_distance_so_far"),
    )
)

qualified = (
    sequenced
    .crossJoin(sequenced.agg(F.min("is_international_row_count_avg_row_count_distanc").alias("overall_min_is_international_row_count_avg_row_c")))
    .filter(F.col("is_international_row_count_avg_row_count_distanc") > F.col("overall_min_is_international_row_count_avg_row_c"))
    .drop("overall_min_is_international_row_count_avg_row_c")
)

metrics = (
    qualified
    .select(
        "*",
        F.round(F.col("row_count_distance") - F.avg("row_count_distance").over(w_row_count), 2).alias("row_count_distance_vs_row_count_avg"),
        F.rank().over(w_is_international_by_row_coun).alias("row_count_distance_rank"),
        F.max("row_count_distance").over(w_is_international_by_row_coun_2).alias("row_count_distance_max_7"),
        F.count(F.lit(1)).over(w_row_count).alias("rows_in_row_count"),
        F.row_number().over(w_is_international_by_row_coun).alias("rn"),
    )
)

windowed = (
    metrics
    .select(
        "is_international",
        "row_count",
        "gpa_is_international_true",
        "row_count_distance",
        "is_international_row_count_avg_row_count_distanc",
        "last_gpa_is_international_true",
        "max_row_count_distance_so_far",
        "row_count_distance_vs_row_count_avg",
        "row_count_distance_max_7",
        "rows_in_row_count",
        "rn",
        F.round(F.cume_dist().over(w_rn_by_row_count_distance_max), 3).alias("row_count_distance_max_7_cume_dist"),
        F.last("gpa_is_international_true").over(w_is_international_by_is_inter).alias("last_gpa_is_international_true_2"),
        F.sum("row_count_distance_max_7").over(w_row_count_distance_rank_row).alias("row_count_distance_rank_row_count_distance_sum_r"),
        F.round(F.percent_rank().over(w_rn_by_row_count_distance_max_2), 3).alias("row_count_distance_max_7_percent_rank"),
        F.nth_value("gpa_is_international_true", 2).over(w_is_international_by_is_inter).alias("second_gpa_is_international_true"),
    )
)

result = (
    windowed
    .cube("rn", "row_count", "row_count_distance")
    .agg(
        F.round(F.avg("is_international_row_count_avg_row_count_distanc"), 2).alias("avg_is_international_row_count_avg_row_count_dis"),
        F.round(F.avg("rows_in_row_count"), 2).alias("avg_rows_in_row_count"),
        F.sum("row_count").alias("total_row_count"),
    )
    .orderBy(
        F.col("rn").asc_nulls_last(),
        F.col("row_count").asc_nulls_last(),
        F.col("row_count_distance").asc_nulls_last(),
    )
)
