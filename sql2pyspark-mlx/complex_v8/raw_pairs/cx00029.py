from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_birth_year = Window.orderBy("birth_year", "id")
w_signup_date_plus_12m_month_b = Window.partitionBy(F.date_trunc("month", "signup_date_plus_12m")).orderBy(F.col("signup_date_plus_12m").desc(), "id")
w_signup_date_year_signup_date = Window.partitionBy(F.year("signup_date"), F.date_trunc("month", "signup_date_plus_12m"))

prepared = (
    df_listeners
    .filter(~F.array_contains(F.col("interests"), "kpop"))
    .select(
        "id",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
        F.add_months(F.col("signup_date"), 12).alias("signup_date_plus_12m"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        (F.col("birth_year") - F.lag("birth_year").over(w_all_by_birth_year)).alias("birth_year_change"),
        F.ntile(5).over(w_signup_date_plus_12m_month_b).alias("signup_date_plus_12m_quintile"),
        F.max("birth_year").over(w_signup_date_year_signup_date).alias("signup_date_year_signup_date_plus_12m_month_max_"),
    )
)

plan_stats = (
    with_trends
    .groupBy("plan")
    .agg(F.avg("birth_year").alias("avg_birth_year"))
)

vs_group = (
    with_trends.alias("s")
    .join(plan_stats.alias("g"), F.col("g.plan") == F.col("s.plan"), "inner")
    .filter(F.col("s.birth_year") > F.col("g.avg_birth_year"))
    .select(
        "s.*",
        "g.avg_birth_year",
        F.round(F.col("s.birth_year") - F.col("g.avg_birth_year"), 2).alias("birth_year_minus_avg"),
    )
)

exploded = (
    vs_group
    .select(
        "id",
        "plan",
        "signup_date",
        "birth_year",
        "signup_date_plus_12m",
        "birth_year_change",
        "signup_date_plus_12m_quintile",
        "signup_date_year_signup_date_plus_12m_month_max_",
        "avg_birth_year",
        "birth_year_minus_avg",
        F.explode("interests").alias("interest"),
    )
)

labeled = exploded.select("*", F.substring(F.trim(F.col("plan")), 1, 3).alias("plan_prefix"))

result = (
    labeled
    .select(
        "id",
        "interest",
        F.col("birth_year_minus_avg").alias("m_birth_year_minus_avg"),
        F.col("avg_birth_year").alias("avg_birth_year_v"),
    )
    .unpivot(
        ids=["id", "interest"],
        values=["m_birth_year_minus_avg", "avg_birth_year_v"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
