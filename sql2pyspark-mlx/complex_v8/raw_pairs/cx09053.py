from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_stddev_birth_year_run = Window.orderBy("stddev_birth_year", "signup_date_month", "country").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_countrys = Window.partitionBy("distinct_countrys")
w_all_by_stddev_birth_year_mov = Window.orderBy("stddev_birth_year", "signup_date_month", "country").rowsBetween(-6, Window.currentRow)
w_country_group_by_distinct_co = Window.partitionBy("country_group").orderBy("distinct_countrys", "signup_date_month", "country").rowsBetween(-6, -1)
w_all_by_stddev_birth_year_pri = Window.orderBy("stddev_birth_year", "signup_date_month", "country").rowsBetween(-6, -1)

valid = (
    df_listeners
    .filter((F.upper(F.trim(F.col("username"))).like("A%")) & (~F.col("plan").isin("premium", "free")))
    .select(
        "id",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.lower(F.regexp_replace(F.trim(F.col("country")), " ", "_")).alias("country_slug"),
    )
)

per_group = (
    valid
    .groupBy(
        F.date_trunc("month", "signup_date").alias("signup_date_month"),
        "country",
    )
    .agg(
        F.countDistinct("country").alias("distinct_countrys"),
        F.min("signup_date").alias("first_signup_date"),
        F.round(F.stddev("birth_year"), 2).alias("stddev_birth_year"),
    )
)

derived = (
    per_group
    .filter(F.col("stddev_birth_year") <= 1506.0)
    .select(
        "signup_date_month",
        "country",
        "distinct_countrys",
        "first_signup_date",
        "stddev_birth_year",
        F.when(F.col("country").isin("JP", "TH"), "group_a").when(F.col("country") == "BR", "group_b").otherwise("other").alias("country_group"),
    )
)

ranked = (
    derived
    .select(
        "signup_date_month",
        "country",
        "stddev_birth_year",
        "country_group",
        F.min("stddev_birth_year").over(w_all_by_stddev_birth_year_run).alias("min_stddev_birth_year_so_far"),
        F.max("distinct_countrys").over(w_distinct_countrys).alias("distinct_countrys_max_distinct_countrys"),
        F.sum("distinct_countrys").over(w_all_by_stddev_birth_year_mov).alias("distinct_countrys_sum_7"),
        F.round(F.avg("stddev_birth_year").over(w_country_group_by_distinct_co), 2).alias("stddev_birth_year_avg_6_prior"),
        F.sum("distinct_countrys").over(w_all_by_stddev_birth_year_pri).alias("distinct_countrys_sum_6_prior"),
    )
)

result = (
    ranked
    .select(
        "signup_date_month",
        "country",
        F.col("distinct_countrys_max_distinct_countrys").alias("distinct_countrys_max_distinct_countrys_v"),
        F.col("distinct_countrys_sum_7").alias("m_distinct_countrys_sum_7"),
        F.col("distinct_countrys_sum_6_prior").alias("m_distinct_countrys_sum_6_prior"),
    )
    .unpivot(
        ids=["signup_date_month", "country"],
        values=["distinct_countrys_max_distinct_countrys_v", "m_distinct_countrys_sum_7", "m_distinct_countrys_sum_6_prior"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
