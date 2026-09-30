from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_international = Window.partitionBy("is_international")
w_enrolled_on_year_is_internat = Window.partitionBy(F.year("enrolled_on"), "is_international").orderBy(F.col("gpa").desc())
w_all_by_total_gpa_rank_full = Window.orderBy("total_gpa_rank", "enrolled_on_quarter", "is_international", "enrolled_on_year").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_on_quarter_row_coun = Window.partitionBy("enrolled_on_quarter", "row_count").orderBy(F.col("row_count").desc())
w_all_by_total_gpa_rank = Window.orderBy("total_gpa_rank", "enrolled_on_quarter", "is_international", "enrolled_on_year")
w_row_count = Window.partitionBy("row_count")

filtered_students = (
    df_students
    .filter(F.col("enrolled_on") >= F.lit("2024-04-05").cast("date"))
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.quarter(F.col("enrolled_on")).alias("enrolled_on_quarter"),
    )
)

sequenced = (
    filtered_students
    .select(
        "*",
        F.min("gpa").over(w_is_international).alias("is_international_min_gpa"),
        F.round(F.col("enrolled_on_quarter") - F.avg("enrolled_on_quarter").over(w_is_international), 2).alias("enrolled_on_quarter_vs_is_international_avg"),
        F.rank().over(w_enrolled_on_year_is_internat).alias("gpa_rank"),
    )
)

summary = (
    sequenced
    .filter(F.col("major") != "Art")
    .groupBy(
        "enrolled_on_quarter",
        "is_international",
        F.year("enrolled_on").alias("enrolled_on_year"),
    )
    .agg(
        F.array_sort(F.collect_set("student_name")).alias("student_name_list"),
        F.count("*").alias("row_count"),
        F.sum("gpa_rank").alias("total_gpa_rank"),
    )
)

sequenced_2 = (
    summary
    .select(
        "*",
        F.nth_value("row_count", 2).over(w_all_by_total_gpa_rank_full).alias("second_row_count"),
        F.round(F.cume_dist().over(w_enrolled_on_quarter_row_coun), 3).alias("row_count_cume_dist"),
        F.row_number().over(w_all_by_total_gpa_rank).alias("total_gpa_rank_seq"),
        F.round(F.col("enrolled_on_year") / F.nullif(F.sum("enrolled_on_year").over(w_row_count), F.lit(0)), 4).alias("enrolled_on_year_share_of_row_count"),
        F.min("enrolled_on_quarter").over(w_row_count).alias("row_count_min_enrolled_on_quarter"),
        F.count(F.lit(1)).over(w_row_count).alias("rows_in_row_count"),
    )
)

flattened = (
    sequenced_2
    .select(
        "enrolled_on_quarter",
        "is_international",
        "enrolled_on_year",
        "row_count",
        "total_gpa_rank",
        "second_row_count",
        "row_count_cume_dist",
        "total_gpa_rank_seq",
        "enrolled_on_year_share_of_row_count",
        "row_count_min_enrolled_on_quarter",
        "rows_in_row_count",
        F.explode_outer("student_name_list").alias("student_name_list_item"),
    )
)

qualified = (
    flattened
    .crossJoin(flattened.agg(F.avg("row_count_min_enrolled_on_quarter").alias("overall_avg_row_count_min_enrolled_on_quarter")))
    .filter(F.col("row_count_min_enrolled_on_quarter") > F.col("overall_avg_row_count_min_enrolled_on_quarter"))
    .drop("overall_avg_row_count_min_enrolled_on_quarter")
)

qualified_2 = (
    qualified
    .crossJoin(qualified.agg(F.min("second_row_count").alias("overall_min_second_row_count")))
    .filter(F.col("second_row_count") > F.col("overall_min_second_row_count"))
    .drop("overall_min_second_row_count")
)

result = (
    qualified_2
    .select(
        "enrolled_on_quarter",
        "is_international",
        "enrolled_on_year",
        "total_gpa_rank",
        "second_row_count",
        "row_count_cume_dist",
        "total_gpa_rank_seq",
        "enrolled_on_year_share_of_row_count",
        "row_count_min_enrolled_on_quarter",
        "rows_in_row_count",
        "student_name_list_item",
        F.round(F.col("row_count") / F.nullif(F.col("total_gpa_rank_seq"), F.lit(0)), 3).alias("row_count_per_total_gpa_rank_seq"),
        (F.col("enrolled_on_year") - F.col("total_gpa_rank")).alias("enrolled_on_year_minus_total_gpa_rank"),
    )
    .orderBy(
        F.col("row_count_cume_dist").desc(),
        F.col("enrolled_on_quarter").desc_nulls_first(),
    )
)
