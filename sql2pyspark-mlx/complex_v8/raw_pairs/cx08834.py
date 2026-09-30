from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_international_distinct_co = Window.partitionBy("is_international", "distinct_cohorts")
w_is_international_by_distinct = Window.partitionBy("is_international").orderBy("distinct_cohorts")

valid = (
    df_students
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "gpa",
        "is_international",
        F.abs(F.col("gpa") - 2.7).alias("gpa_distance"),
        F.date_trunc("week", F.col("enrolled_on")).alias("enrolled_on_week"),
    )
)

totals = (
    valid
    .filter(F.col("gpa") >= 2.0)
    .groupBy(
        F.date_trunc("month", "enrolled_on").alias("enrolled_on_month"),
        "is_international",
        "cohort",
    )
    .agg(
        F.countDistinct("cohort").alias("distinct_cohorts"),
        F.max("enrolled_on").alias("last_enrolled_on"),
    )
)

with_windows = (
    totals
    .select(
        "*",
        F.round(F.col("distinct_cohorts") - F.avg("distinct_cohorts").over(w_is_international_distinct_co), 2).alias("distinct_cohorts_vs_is_international_distinct_co"),
        F.round(F.col("distinct_cohorts") / F.nullif(F.sum("distinct_cohorts").over(w_is_international_distinct_co), F.lit(0)), 4).alias("distinct_cohorts_share_of_is_international_disti"),
        F.round(F.percent_rank().over(w_is_international_by_distinct), 3).alias("distinct_cohorts_percent_rank"),
    )
)

labeled = (
    with_windows
    .withColumn("cohort_group", F.when(F.col("cohort").isin("C2021", "C2024"), "group_a").when(F.col("cohort") == "C2023", "group_b").otherwise("other"))
)

result = (
    labeled
    .select(
        "enrolled_on_month",
        "is_international",
        "distinct_cohorts_percent_rank",
        "distinct_cohorts_share_of_is_international_disti",
        "distinct_cohorts_vs_is_international_distinct_co",
    )
    .unpivot(
        ids=["enrolled_on_month", "is_international"],
        values=["distinct_cohorts_percent_rank", "distinct_cohorts_share_of_is_international_disti", "distinct_cohorts_vs_is_international_distinct_co"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
