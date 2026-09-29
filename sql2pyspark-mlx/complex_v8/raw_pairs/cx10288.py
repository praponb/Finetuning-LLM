from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_enrolled_o = Window.orderBy("days_since_enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_gpa_prior_1 = Window.orderBy("gpa", "id").rowsBetween(-1, -1)
w_enrolled_on_month_month_coho = Window.partitionBy(F.month("enrolled_on_month"), "cohort")
w_is_international_by_gpa = Window.partitionBy("is_international").orderBy(F.col("gpa").desc(), "id")
w_all_by_gpa_full = Window.orderBy("gpa", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_international = Window.partitionBy("is_international")

prepared_students = (
    df_students
    .filter((F.upper(F.trim(F.col("student_name"))).like("A%")) & (F.col("enrolled_on") < F.lit("2025-07-18").cast("date")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.date_trunc("month", F.col("enrolled_on")).alias("enrolled_on_month"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_on")).alias("days_since_enrolled_on"),
    )
)

ranked = (
    prepared_students
    .select(
        "*",
        F.sum("days_since_enrolled_on").over(w_all_by_days_since_enrolled_o).alias("running_days_since_enrolled_on"),
        F.min("gpa").over(w_all_by_days_since_enrolled_o).alias("min_gpa_so_far"),
        F.max("gpa").over(w_all_by_days_since_enrolled_o).alias("max_gpa_so_far"),
    )
)

ranked_2 = (
    ranked
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        "enrolled_on_month",
        "days_since_enrolled_on",
        "min_gpa_so_far",
        "max_gpa_so_far",
        F.max("days_since_enrolled_on").over(w_all_by_gpa_prior_1).alias("days_since_enrolled_on_max_1_prior"),
        F.count(F.lit(1)).over(w_enrolled_on_month_month_coho).alias("rows_in_enrolled_on_month_month_cohort"),
        F.round(F.col("gpa") / F.nullif(F.sum("gpa").over(w_enrolled_on_month_month_coho), F.lit(0)), 4).alias("gpa_share_of_enrolled_on_month_month_cohort"),
        F.lead("gpa").over(w_is_international_by_gpa).alias("next_gpa"),
        F.nth_value("gpa", 2).over(w_all_by_gpa_full).alias("second_gpa"),
    )
)

with_trends = (
    ranked_2
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        "is_international",
        "enrolled_on_month",
        "days_since_enrolled_on",
        "min_gpa_so_far",
        "max_gpa_so_far",
        "days_since_enrolled_on_max_1_prior",
        "rows_in_enrolled_on_month_month_cohort",
        "next_gpa",
        "second_gpa",
        F.round(F.col("min_gpa_so_far") - F.avg("min_gpa_so_far").over(w_is_international), 2).alias("min_gpa_so_far_vs_is_international_avg"),
        F.round(F.col("gpa") / F.nullif(F.sum("gpa").over(w_is_international), F.lit(0)), 4).alias("gpa_share_of_is_international"),
    )
)

result = (
    with_trends
    .select("id", "days_since_enrolled_on_max_1_prior", "days_since_enrolled_on")
    .unpivot(
        ids=["id"],
        values=["days_since_enrolled_on_max_1_prior", "days_since_enrolled_on"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .orderBy("id", "metric")
)
