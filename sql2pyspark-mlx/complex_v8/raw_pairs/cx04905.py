from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_days_since_signup_dat = Window.orderBy("days_since_signup_date", "id")
w_all_by_signup_date = Window.orderBy("signup_date", "id")
w_latest = Window.partitionBy("country").orderBy(F.col("next_signup_date").desc(), F.col("id").desc())
w_signup_date_month_by_birth_y = Window.partitionBy(F.date_trunc("month", "signup_date")).orderBy("birth_year_bucket", "id").rowsBetween(-1, -1)
w_prev_signup_date_month_next = Window.partitionBy(F.month("prev_signup_date"), F.month("next_signup_date"))
w_prev_signup_date_year_by_nex = Window.partitionBy(F.year("prev_signup_date")).orderBy("next_signup_date", "id")
w_prev_signup_date_year_by_nex_2 = Window.partitionBy(F.year("prev_signup_date")).orderBy("next_signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base = (
    df_listeners
    .filter(F.col("birth_year") < 1981)
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "birth_year",
        "interests",
    )
)

labeled = (
    base
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "birth_year",
        (F.floor(F.col("birth_year") / 10) * 10).alias("birth_year_bucket"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("signup_date")).alias("days_since_signup_date"),
    )
)

windowed = (
    labeled
    .withColumn("prev_signup_date", F.lag("signup_date").over(w_all_by_days_since_signup_dat))
    .withColumn("next_signup_date", F.lead("signup_date").over(w_all_by_signup_date))
)

unique_rows = (
    windowed
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

sequenced = (
    unique_rows
    .select(
        "id",
        "username",
        "country",
        "days_since_signup_date",
        "next_signup_date",
        F.round(F.avg("days_since_signup_date").over(w_signup_date_month_by_birth_y), 2).alias("days_since_signup_date_avg_1_prior"),
        F.round(F.col("days_since_signup_date") - F.avg("days_since_signup_date").over(w_prev_signup_date_month_next), 2).alias("days_since_signup_date_vs_prev_signup_date_month"),
        F.round(F.col("days_since_signup_date") / F.nullif(F.sum("days_since_signup_date").over(w_prev_signup_date_month_next), F.lit(0)), 4).alias("days_since_signup_date_share_of_prev_signup_date"),
        F.ntile(5).over(w_prev_signup_date_year_by_nex).alias("next_signup_date_quintile"),
        F.nth_value("prev_signup_date", 2).over(w_prev_signup_date_year_by_nex_2).alias("second_prev_signup_date"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "days_since_signup_date_share_of_prev_signup_date",
        "days_since_signup_date_avg_1_prior",
        "days_since_signup_date_vs_prev_signup_date_month",
    )
    .unpivot(
        ids=["id"],
        values=["days_since_signup_date_share_of_prev_signup_date", "days_since_signup_date_avg_1_prior", "days_since_signup_date_vs_prev_signup_date_month"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "metric")
)
