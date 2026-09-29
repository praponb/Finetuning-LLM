from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_signup_date_year_username_le = Window.partitionBy(F.year("signup_date"), "username_len").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_signup_date_plus_30d_year_by = Window.partitionBy(F.year("signup_date_plus_30d")).orderBy(F.col("birth_year").desc())
w_plan_by_birth_year_full = Window.partitionBy("plan").orderBy("birth_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_plan = Window.partitionBy("plan")

valid_listeners = (
    df_listeners
    .filter((F.col("signup_date").isNotNull()) & (F.col("country") == "UK"))
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
        "birth_year",
        "interests",
        F.date_add(F.col("signup_date"), 30).alias("signup_date_plus_30d"),
        F.length(F.trim(F.col("username"))).alias("username_len"),
    )
)

with_windows = (
    valid_listeners
    .select(
        "id",
        "username",
        "plan",
        "birth_year",
        "interests",
        "username_len",
        F.first("signup_date", ignorenulls=True).over(w_signup_date_year_username_le).alias("first_known_signup_date"),
        F.round(F.percent_rank().over(w_signup_date_plus_30d_year_by), 3).alias("birth_year_percent_rank"),
    )
)

with_trends = (
    with_windows
    .select(
        "id",
        "plan",
        "interests",
        "username_len",
        "first_known_signup_date",
        "birth_year_percent_rank",
        F.first("username_len").over(w_plan_by_birth_year_full).alias("first_username_len"),
        F.count(F.lit(1)).over(w_plan).alias("rows_in_plan"),
    )
)

result = (
    with_trends
    .select("id", "rows_in_plan", "first_username_len")
    .unpivot(
        ids=["id"],
        values=["rows_in_plan", "first_username_len"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
