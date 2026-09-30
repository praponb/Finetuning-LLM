from pyspark.sql import functions as F

base = (
    df_students
    .filter((F.col("enrolled_on").between(F.lit("2024-04-01").cast("date"), F.lit("2025-06-27").cast("date"))) | (F.col("major").isin("Math", "Bio", "Biz")))
    .select(
        "id",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.last_day(F.col("enrolled_on")).alias("enrolled_on_month_end"),
        F.concat_ws("-", F.col("major"), F.col("student_name")).alias("major_student_name_label"),
        F.when(F.col("is_international"), "yes").otherwise("no").alias("is_international_label"),
    )
)

derived = (
    base
    .filter(F.year(F.col("enrolled_on_month_end")) == 2024)
    .select(
        "*",
        F.when(F.col("gpa") >= 3.4, "large").when(F.col("gpa") >= 2.7, "medium").otherwise("small").alias("gpa_tier"),
    )
)

derived_2 = (
    derived
    .filter(F.col("major_student_name_label").isNotNull())
    .select(
        "*",
        F.when(F.col("is_international"), "yes").otherwise("no").alias("is_international_label_2"),
        F.coalesce(F.col("gpa"), F.lit(0.0)).alias("gpa_filled"),
    )
)

is_international_label_stats = (
    derived_2
    .groupBy("is_international_label")
    .agg(F.max("gpa").alias("max_gpa"))
)

compared = (
    derived_2.alias("s")
    .join(is_international_label_stats.alias("g"), F.col("g.is_international_label") == F.col("s.is_international_label"), "inner")
    .filter(F.col("s.gpa") == F.col("g.max_gpa"))
    .select(
        "s.*",
        "g.max_gpa",
        F.round(F.col("s.gpa") - F.col("g.max_gpa"), 2).alias("gpa_minus_max"),
    )
)

result = (
    compared
    .rollup("is_international_label_2", "cohort", "gpa_tier")
    .agg(
        F.round(F.stddev("gpa"), 2).alias("stddev_gpa"),
        F.sum(F.when(F.col("major").isNotNull(), 1).otherwise(0)).alias("n_major_is_not_null"),
        F.round(F.sum("gpa_filled") / F.nullif(F.sum("max_gpa"), F.lit(0)), 3).alias("gpa_filled_to_max_gpa_ratio"),
    )
    .select(
        F.coalesce(F.col("is_international_label_2"), F.lit("ALL")).alias("is_international_label_2"),
        F.coalesce(F.col("cohort"), F.lit("ALL")).alias("cohort"),
        F.coalesce(F.col("gpa_tier"), F.lit("ALL")).alias("gpa_tier"),
        "stddev_gpa",
        "n_major_is_not_null",
        "gpa_filled_to_max_gpa_ratio",
    )
)
