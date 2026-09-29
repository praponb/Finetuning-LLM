from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_major_by_enrolled_on_ties = Window.partitionBy("major").orderBy(F.col("enrolled_on").desc_nulls_first())
w_major_is_international_by_en = Window.partitionBy("major", "is_international").orderBy("enrolled_on", "id")
w_all_by_enrolled_on_ties = Window.orderBy("enrolled_on")
w_is_international_by_enrolled = Window.partitionBy("is_international").orderBy("enrolled_on", "id")
w_enrolled_on_month_by_enrolle = Window.partitionBy(F.month("enrolled_on")).orderBy(F.col("enrolled_on").desc())
w_is_international_by_enrolled_2 = Window.partitionBy("is_international").orderBy("enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_major_by_enrolled_on = Window.partitionBy("major").orderBy("enrolled_on", "id")
w_major_is_international_by_en_2 = Window.partitionBy("major", "is_international").orderBy("enrolled_on")
w_is_international_by_enrolled_3 = Window.partitionBy("is_international").orderBy("enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_enrolled_on_month_by_enrolle_2 = Window.partitionBy(F.month("enrolled_on")).orderBy(F.col("enrolled_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_international_label = Window.partitionBy("is_international_label")
w_enrolled_on_dense_rank_by_en = Window.partitionBy("enrolled_on_dense_rank").orderBy(F.col("enrolled_on_percent_rank").desc(), "id")
w_is_international_enrolled_on = Window.partitionBy("is_international", "enrolled_on_dense_rank")

recent = (
    df_students
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "is_international",
        F.concat(F.col("student_name"), F.lit("#"), F.col("id").cast("string")).alias("student_name_ref"),
    )
)

sequenced = (
    recent
    .select(
        "*",
        F.dense_rank().over(w_major_by_enrolled_on_ties).alias("enrolled_on_dense_rank"),
        F.row_number().over(w_major_is_international_by_en).alias("rn"),
        F.round(F.cume_dist().over(w_all_by_enrolled_on_ties), 3).alias("enrolled_on_cume_dist"),
        F.row_number().over(w_is_international_by_enrolled).alias("enrolled_on_seq"),
        F.round(F.percent_rank().over(w_enrolled_on_month_by_enrolle), 3).alias("enrolled_on_percent_rank"),
        F.first("enrolled_on", ignorenulls=True).over(w_is_international_by_enrolled_2).alias("first_known_enrolled_on"),
        F.rank().over(w_major_by_enrolled_on).alias("enrolled_on_rank"),
        F.round(F.percent_rank().over(w_major_is_international_by_en_2), 3).alias("enrolled_on_percent_rank_2"),
        F.nth_value("enrolled_on", 2).over(w_is_international_by_enrolled_3).alias("second_enrolled_on"),
        F.nth_value("enrolled_on", 2).over(w_enrolled_on_month_by_enrolle_2).alias("second_enrolled_on_2"),
        F.datediff(F.col("enrolled_on"), F.lag("enrolled_on").over(w_is_international_by_enrolled)).alias("days_since_prev_enrolled_on"),
        F.row_number().over(w_major_by_enrolled_on).alias("enrolled_on_seq_2"),
    )
)

scored = (
    sequenced
    .filter(F.col("enrolled_on") >= F.lit("2024-04-01").cast("date"))
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "is_international",
        "student_name_ref",
        "enrolled_on_dense_rank",
        "enrolled_on_cume_dist",
        "enrolled_on_seq",
        "enrolled_on_percent_rank",
        "enrolled_on_percent_rank_2",
        "second_enrolled_on",
        "second_enrolled_on_2",
        "days_since_prev_enrolled_on",
        "enrolled_on_seq_2",
        F.when(F.col("is_international"), "yes").otherwise("no").alias("is_international_label"),
        F.quarter(F.col("second_enrolled_on_2")).alias("second_enrolled_on_2_quarter"),
    )
)

sequenced_2 = (
    scored
    .select(
        "*",
        F.round(F.avg("enrolled_on_percent_rank").over(w_is_international_label), 2).alias("is_international_label_avg_enrolled_on_percent_r"),
        F.round(F.col("second_enrolled_on_2_quarter") / F.nullif(F.sum("second_enrolled_on_2_quarter").over(w_is_international_label), F.lit(0)), 4).alias("second_enrolled_on_2_quarter_share_of_is_interna"),
        F.min("enrolled_on_dense_rank").over(w_is_international_label).alias("is_international_label_min_enrolled_on_dense_ran"),
        F.datediff(F.col("second_enrolled_on"), F.lag("second_enrolled_on").over(w_enrolled_on_dense_rank_by_en)).alias("days_since_prev_second_enrolled_on"),
        F.size(F.collect_set("major").over(w_is_international_enrolled_on)).alias("distinct_major_in_is_international_enrolled_on_d"),
        F.size(F.collect_set("is_international_label").over(w_is_international_enrolled_on)).alias("distinct_is_international_label_in_is_internatio"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "is_international",
        "enrolled_on_dense_rank",
        "enrolled_on_cume_dist",
        "enrolled_on_seq",
        "enrolled_on_percent_rank",
        "enrolled_on_percent_rank_2",
        "enrolled_on_seq_2",
        "is_international_label",
        "second_enrolled_on_2_quarter",
        "is_international_label_avg_enrolled_on_percent_r",
        "second_enrolled_on_2_quarter_share_of_is_interna",
        "is_international_label_min_enrolled_on_dense_ran",
        "days_since_prev_second_enrolled_on",
        "distinct_major_in_is_international_enrolled_on_d",
        "distinct_is_international_label_in_is_internatio",
    )
    .orderBy(F.col("distinct_is_international_label_in_is_internatio").desc())
)
