from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_plan_or_unknown_by_signup_da = Window.partitionBy("plan_or_unknown").orderBy(F.col("signup_date_quarter_date").desc())
w_plan_or_unknown_2_by_signup = Window.partitionBy("plan_or_unknown_2").orderBy("signup_date_quarter_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_plan_or_unknown_2_or_unknown = Window.partitionBy("plan_or_unknown_2_or_unknown", "plan")

filtered = (
    df_listeners
    .filter(F.array_contains(F.col("interests"), "indie"))
    .select(
        "id",
        "username",
        "plan",
        "interests",
        F.coalesce(F.col("plan"), F.lit("UNKNOWN")).alias("plan_or_unknown"),
        F.date_trunc("quarter", F.col("signup_date")).alias("signup_date_quarter"),
        F.coalesce(F.col("plan"), F.lit("UNKNOWN")).alias("plan_or_unknown_2"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "username",
        "plan",
        "interests",
        "plan_or_unknown",
        "plan_or_unknown_2",
        F.concat_ws("-", F.col("plan_or_unknown_2"), F.col("plan_or_unknown")).alias("plan_or_unknown_2_plan_or_unknown_label"),
        F.to_date(F.col("signup_date_quarter")).alias("signup_date_quarter_date"),
        F.substring(F.trim(F.col("plan_or_unknown_2")), 1, 2).alias("plan_or_unknown_2_prefix"),
    )
)

tagged = (
    enriched
    .select(
        "*",
        F.coalesce(F.col("plan_or_unknown_2"), F.lit("UNKNOWN")).alias("plan_or_unknown_2_or_unknown"),
        F.substring(F.trim(F.col("username")), 1, 4).alias("username_prefix"),
        F.size(F.col("interests")).alias("interests_count"),
    )
)

windowed = (
    tagged
    .select(
        "*",
        F.round(F.cume_dist().over(w_plan_or_unknown_by_signup_da), 3).alias("signup_date_quarter_date_cume_dist"),
        F.sum("interests_count").over(w_plan_or_unknown_2_by_signup).alias("running_interests_count"),
        F.count(F.lit(1)).over(w_plan_or_unknown_2_or_unknown).alias("rows_in_plan_or_unknown_2_or_unknown_plan"),
        F.min("interests_count").over(w_plan_or_unknown_2_or_unknown).alias("plan_or_unknown_2_or_unknown_plan_min_interests_"),
        F.size(F.collect_set("plan_or_unknown_2").over(w_plan_or_unknown_2_or_unknown)).alias("distinct_plan_or_unknown_2_in_plan_or_unknown_2_"),
    )
)

result = (
    windowed
    .rollup("plan_or_unknown_2_or_unknown", "plan_or_unknown_2", "distinct_plan_or_unknown_2_in_plan_or_unknown_2_")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.length(F.trim(F.col("username"))) >= 5, F.col("signup_date_quarter_date_cume_dist"))).alias("signup_date_quarter_date_cume_dist_length_trim_u"),
        F.count("*").alias("n_rows"),
    )
    .select(
        F.coalesce(F.col("plan_or_unknown_2_or_unknown"), F.lit("ALL")).alias("plan_or_unknown_2_or_unknown"),
        F.coalesce(F.col("plan_or_unknown_2"), F.lit("ALL")).alias("plan_or_unknown_2"),
        "distinct_plan_or_unknown_2_in_plan_or_unknown_2_",
        "gid",
        "signup_date_quarter_date_cume_dist_length_trim_u",
        "n_rows",
    )
)
