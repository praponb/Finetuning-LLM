from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_major_cohort = Window.partitionBy("major", "cohort")
w_is_international_label_is_in = Window.partitionBy("is_international_label", "is_international").orderBy("gpa", "id")

base_students = (
    df_students
    .filter(F.col("is_international"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.when(F.col("is_international"), "yes").otherwise("no").alias("is_international_label"),
    )
)

with_trends = (
    base_students
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "is_international",
        "is_international_label",
        F.count(F.lit(1)).over(w_major_cohort).alias("rows_in_major_cohort"),
        (F.col("gpa") - F.lag("gpa").over(w_is_international_label_is_in)).alias("gpa_change"),
    )
)

annotated = (
    with_trends
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "rows_in_major_cohort",
        "gpa_change",
        F.date_add(F.col("enrolled_on"), 365).alias("enrolled_on_plus_365d"),
    )
)

grouped = (
    annotated
    .groupBy(
        "major",
        F.when(F.col("gpa_change") >= 1.6, "HIGH").when(F.col("gpa_change") >= -0.8, "MEDIUM").otherwise("LOW").alias("gpa_change_tier"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("student_name"))).like("G%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_student_name_like"),
        F.sum("rows_in_major_cohort").alias("total_rows_in_major_cohort"),
        F.array_sort(F.collect_set("student_name")).alias("student_name_list"),
    )
    .filter(F.col("total_rows_in_major_cohort") > 6)
)

enriched = (
    grouped
    .filter(F.size(F.col("student_name_list")) >= 2)
    .select(
        "major",
        "gpa_change_tier",
        "pct_upper_trim_student_name_like",
        "student_name_list",
        F.length(F.trim(F.col("gpa_change_tier"))).alias("gpa_change_tier_len"),
        F.lower(F.regexp_replace(F.trim(F.col("gpa_change_tier")), " ", "_")).alias("gpa_change_tier_slug"),
    )
)

scored = (
    enriched
    .filter(F.size(F.col("student_name_list")) >= 1)
    .select(
        "*",
        F.coalesce(F.col("gpa_change_tier_len"), F.lit(0)).alias("gpa_change_tier_len_filled"),
        F.when(F.col("gpa_change_tier").isin("HIGH", "MEDIUM"), "group_a").when(F.col("gpa_change_tier") == "LOW", "group_b").otherwise("other").alias("gpa_change_tier_group"),
        F.when(F.col("gpa_change_tier_len") >= 12, "HIGH").when(F.col("gpa_change_tier_len") >= 5, "MEDIUM").otherwise("LOW").alias("gpa_change_tier_len_tier"),
    )
)

result = (
    scored
    .select(
        "major",
        "gpa_change_tier",
        "gpa_change_tier_len",
        "gpa_change_tier_len_filled",
    )
    .unpivot(
        ids=["major", "gpa_change_tier"],
        values=["gpa_change_tier_len", "gpa_change_tier_len_filled"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .orderBy("major", "gpa_change_tier", "field")
)
