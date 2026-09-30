from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "country").orderBy("monthly_fee", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("start_date", "id")
w3 = Window.partitionBy(F.month("start_date"), "country").orderBy("monthly_fee", "id")
w4 = Window.partitionBy("plan").orderBy("end_date", "id")
w5 = Window.partitionBy("status").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("customer_id").orderBy("signup_date", "id")
w7 = Window.partitionBy("customer_id").orderBy("prev_monthly_fee", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.partitionBy("plan").orderBy("seq_no", "id")
w9 = Window.partitionBy("plan").orderBy("start_date", "id")

joined = (
    df_subscriptions
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .filter(F.col("d.age") < 31)
    .select("a.*", F.col("d.signup_date"), F.col("d.country"))
)

cleaned = (
    joined
    .filter(F.col("customer_id").isNotNull())
    .select(
        "*",
        (F.datediff(F.current_date(), "start_date") / 365.25).alias("years_since_start_date"),
        F.coalesce("country", F.lit("OTHER")).alias("country_clean"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "signup_date",
        "years_since_start_date",
        "country_clean",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_5"),
        F.lag("monthly_fee", 1).over(w2).alias("prev_monthly_fee"),
        F.lag("monthly_fee", 1).over(w3).alias("prev_monthly_fee_2"),
        F.lead("years_since_start_date", 1, 0).over(w4).alias("next_years_since_start_date"),
        F.first("country", ignorenulls=True).over(w5).alias("first_known_country"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "plan",
        "start_date",
        "years_since_start_date",
        "monthly_fee_avg_5",
        "prev_monthly_fee_2",
        "next_years_since_start_date",
        F.lead("monthly_fee_avg_5").over(w_ordered).alias("next_monthly_fee_avg_5"),
        F.lead("monthly_fee").over(w_ordered).alias("next_monthly_fee"),
        F.row_number().over(w_ordered).alias("seq_no"),
        F.avg("prev_monthly_fee_2").over(w7).alias("prev_monthly_fee_2_avg_6"),
    )
)

enriched_subscriptions = (
    enriched
    .select(
        "id",
        "plan",
        "start_date",
        "years_since_start_date",
        "monthly_fee_avg_5",
        "prev_monthly_fee_2",
        "next_years_since_start_date",
        "next_monthly_fee_avg_5",
        F.lead("next_years_since_start_date").over(w_grp).alias("next_next_years_since_start_date"),
        F.lead("monthly_fee_avg_5").over(w_grp).alias("next_monthly_fee_avg_5_2"),
        F.row_number().over(w_grp).alias("seq_no_2"),
        F.lag("next_monthly_fee_avg_5", 2, 0).over(w9).alias("prev_next_monthly_fee_avg_5"),
    )
)

result = (
    enriched_subscriptions
    .select(
        "id",
        "plan",
        "next_years_since_start_date",
        "next_next_years_since_start_date",
        "prev_monthly_fee_2",
        "years_since_start_date",
    )
    .unpivot(
        ids=["id", "plan"],
        values=["next_years_since_start_date", "next_next_years_since_start_date", "prev_monthly_fee_2", "years_since_start_date"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
