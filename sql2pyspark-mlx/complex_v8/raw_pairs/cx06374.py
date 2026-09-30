from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_international = Window.partitionBy("is_international")
w_enrolled_on_year_cnt_by_min = Window.partitionBy("enrolled_on_year", "cnt").orderBy("min_days_since_enrolled_on", "is_international", "enrolled_on_year")
w_is_international_by_cnt_movi = Window.partitionBy("is_international").orderBy(F.col("cnt").desc(), "is_international", "enrolled_on_year").rowsBetween(-1, Window.currentRow)
w_all_by_total_days_since_enro = Window.orderBy("total_days_since_enrolled_on_2", "is_international", "enrolled_on_year").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_students = (
    df_students
    .filter((F.col("cohort") == "C2021") & (F.upper(F.trim(F.col("student_name"))).like("D%")))
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "is_international",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_on")).alias("days_since_enrolled_on"),
        F.lower(F.regexp_replace(F.trim(F.col("student_name")), " ", "_")).alias("student_name_slug"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_on")).alias("days_since_enrolled_on_2"),
    )
)

stats = (
    valid_students
    .groupBy("is_international", F.year("enrolled_on").alias("enrolled_on_year"))
    .agg(
        F.datediff(F.max("enrolled_on"), F.min("enrolled_on")).alias("enrolled_on_span_days"),
        F.count("*").alias("cnt"),
        F.sum("days_since_enrolled_on_2").alias("total_days_since_enrolled_on_2"),
        F.min("days_since_enrolled_on").alias("min_days_since_enrolled_on"),
    )
)

with_trends = (
    stats
    .select(
        "is_international",
        "enrolled_on_year",
        "enrolled_on_span_days",
        "total_days_since_enrolled_on_2",
        "min_days_since_enrolled_on",
        F.round(F.col("cnt") - F.avg("cnt").over(w_is_international), 2).alias("cnt_vs_is_international_avg"),
        F.round(F.avg("min_days_since_enrolled_on").over(w_is_international), 2).alias("is_international_avg_min_days_since_enrolled_on"),
        (F.col("min_days_since_enrolled_on") - F.lag("min_days_since_enrolled_on").over(w_enrolled_on_year_cnt_by_min)).alias("min_days_since_enrolled_on_change"),
        F.sum("enrolled_on_span_days").over(w_is_international_by_cnt_movi).alias("enrolled_on_span_days_sum_2"),
        F.nth_value("min_days_since_enrolled_on", 2).over(w_all_by_total_days_since_enro).alias("second_min_days_since_enrolled_on"),
        F.lag("enrolled_on_span_days", 2).over(w_enrolled_on_year_cnt_by_min).alias("prev2_enrolled_on_span_days"),
    )
)

scored = (
    with_trends
    .filter(F.col("second_min_days_since_enrolled_on") <= 560)
    .select(
        "*",
        F.coalesce(F.col("enrolled_on_span_days"), F.lit(0)).alias("enrolled_on_span_days_filled"),
        F.coalesce(F.col("enrolled_on_year"), F.lit(0)).alias("enrolled_on_year_filled"),
        F.least(F.col("enrolled_on_span_days"), F.col("enrolled_on_year")).alias("least_enrolled_on_span_days_enrolled_on_year"),
    )
)

result = (
    scored
    .select(
        "is_international",
        "enrolled_on_year",
        "enrolled_on_span_days",
        "cnt_vs_is_international_avg",
        "min_days_since_enrolled_on_change",
        "enrolled_on_span_days_sum_2",
        "second_min_days_since_enrolled_on",
        "prev2_enrolled_on_span_days",
        "enrolled_on_span_days_filled",
        "enrolled_on_year_filled",
        "least_enrolled_on_span_days_enrolled_on_year",
        (F.col("enrolled_on_year") - F.col("enrolled_on_span_days")).alias("enrolled_on_year_minus_enrolled_on_span_days"),
    )
)
