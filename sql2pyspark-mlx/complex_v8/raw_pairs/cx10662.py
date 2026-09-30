from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_plan_or_unknown_2_plan_or_un = Window.partitionBy("plan_or_unknown_2", "plan_or_unknown")
w_plan_by_country_len_running = Window.partitionBy("plan").orderBy(F.col("country_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_plan_by_country_len = Window.partitionBy("plan").orderBy(F.col("country_len").desc(), "id")

base = (
    df_listeners
    .filter(F.upper(F.trim(F.col("username"))).like("H%"))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.coalesce(F.col("plan"), F.lit("UNKNOWN")).alias("plan_or_unknown"),
        F.length(F.trim(F.col("country"))).alias("country_len"),
    )
)

with_activity = (
    base.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.is_skipped")) & (F.col("p.ms_played") > 160264), "left_semi")
)

tagged = (
    with_activity
    .filter(F.col("country") == "TH")
    .select(
        "id",
        "plan",
        "country",
        "interests",
        "plan_or_unknown",
        "country_len",
        F.coalesce(F.col("plan"), F.lit("UNKNOWN")).alias("plan_or_unknown_2"),
    )
)

screened = (
    tagged.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.id")) & (F.col("p.played_at") < F.lit("2025-02-24 09:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("plan_or_unknown_2") == "UNKNOWN")
)

with_activity_2 = (
    screened.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.id"), "left_semi")
)

with_trends = (
    with_activity_2
    .select(
        "*",
        F.round(F.avg("country_len").over(w_plan_or_unknown_2_plan_or_un), 2).alias("plan_or_unknown_2_plan_or_unknown_avg_country_le"),
        F.max("country_len").over(w_plan_by_country_len_running).alias("max_country_len_so_far"),
        F.count(F.lit(1)).over(w_plan_or_unknown_2_plan_or_un).alias("rows_in_plan_or_unknown_2_plan_or_unknown"),
        F.sum("country_len").over(w_plan_or_unknown_2_plan_or_un).alias("plan_or_unknown_2_plan_or_unknown_sum_country_le"),
        F.row_number().over(w_plan_by_country_len).alias("rn"),
    )
)

tagged_2 = (
    with_trends
    .filter(F.col("plan_or_unknown_2").isin("premium", "free"))
    .withColumn("country_or_unknown", F.coalesce(F.col("country"), F.lit("UNKNOWN")))
)

result = (
    tagged_2
    .select(
        "id",
        "max_country_len_so_far",
        "plan_or_unknown_2_plan_or_unknown_sum_country_le",
        "country_len",
    )
    .unpivot(
        ids=["id"],
        values=["max_country_len_so_far", "plan_or_unknown_2_plan_or_unknown_sum_country_le", "country_len"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "measure")
)
