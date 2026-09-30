from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_signup_date_running = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_signup_date_month_plan_by_si = Window.partitionBy(F.month("signup_date"), "plan").orderBy(F.col("signup_date").desc(), "id")
w_signup_date_month_plan_by_si_2 = Window.partitionBy(F.month("signup_date"), "plan").orderBy(F.col("signup_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_signup_date = Window.orderBy("signup_date", "id")

scoped = (
    df_listeners
    .filter((F.col("signup_date") >= F.lit("2024-08-02").cast("date")) & (F.col("plan") == "family"))
    .select(
        "id",
        "username",
        "plan",
        "signup_date",
    )
)

qualified = (
    scoped.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.ms_played") < 302279), "left_semi")
)

with_activity = (
    qualified.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.ms_played") > 160264), "left_semi")
)

with_windows = (
    with_activity
    .select(
        "*",
        F.first("signup_date", ignorenulls=True).over(w_all_by_signup_date_running).alias("first_known_signup_date"),
        F.lag("signup_date").over(w_signup_date_month_plan_by_si).alias("prev_signup_date"),
        F.nth_value("signup_date", 2).over(w_signup_date_month_plan_by_si_2).alias("second_signup_date"),
        F.lead("signup_date", 2).over(w_all_by_signup_date).alias("next2_signup_date"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "signup_date",
        "prev_signup_date",
        "second_signup_date",
        "next2_signup_date",
    )
    .orderBy(F.col("signup_date").desc(), "id")
    .limit(10)
)
