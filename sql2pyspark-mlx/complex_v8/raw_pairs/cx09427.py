from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_on_month_is_interna = Window.partitionBy(F.month("enrolled_on"), "is_international_flag").orderBy("gpa", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_enrolled_on_month_is_interna_2 = Window.partitionBy(F.month("enrolled_on"), "is_international_flag").orderBy("gpa", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_international_flag = Window.partitionBy("is_international_flag")
w_enrolled_on_month_is_interna_3 = Window.partitionBy(F.month("enrolled_on"), "is_international_flag").orderBy("gpa", "id")
w_enrolled_on_month_is_interna_4 = Window.partitionBy(F.month("enrolled_on"), "is_international_flag").orderBy("gpa", "id").rowsBetween(-6, -1)

base = (
    df_students
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

labeled = (
    base
    .filter(~F.col("is_international"))
    .select(
        "*",
        F.when(F.col("is_international"), 1).otherwise(0).alias("is_international_flag"),
        F.date_add(F.col("enrolled_on"), 90).alias("enrolled_on_plus_90d"),
        F.coalesce(F.col("gpa"), F.lit(0.0)).alias("gpa_filled"),
    )
)

without_matches = (
    labeled
    .crossJoin(labeled.agg(F.avg("is_international_flag").alias("overall_avg_is_international_flag")))
    .filter(F.col("is_international_flag") > F.col("overall_avg_is_international_flag"))
    .drop("overall_avg_is_international_flag")
)

sequenced = (
    without_matches
    .select(
        "*",
        F.last("enrolled_on", ignorenulls=True).over(w_enrolled_on_month_is_interna).alias("last_known_enrolled_on"),
        F.nth_value("enrolled_on", 2).over(w_enrolled_on_month_is_interna_2).alias("second_enrolled_on"),
        F.min("is_international_flag").over(w_is_international_flag).alias("is_international_flag_min_is_international_flag"),
        F.round(F.col("is_international_flag") - F.avg("is_international_flag").over(w_is_international_flag), 2).alias("is_international_flag_vs_is_international_flag_a"),
        F.datediff(F.col("enrolled_on_plus_90d"), F.lag("enrolled_on_plus_90d").over(w_enrolled_on_month_is_interna_3)).alias("days_since_prev_enrolled_on_plus_90d"),
        F.round(F.avg("gpa").over(w_enrolled_on_month_is_interna_4), 2).alias("gpa_avg_6_prior"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "is_international_flag",
        "is_international_flag_min_is_international_flag",
    )
    .unpivot(
        ids=["id"],
        values=["is_international_flag", "is_international_flag_min_is_international_flag"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
