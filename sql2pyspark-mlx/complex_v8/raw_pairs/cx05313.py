from pyspark.sql import functions as F

prepared = (
    df_listeners
    .filter(F.col("signup_date").isNotNull())
    .select(
        "id",
        "username",
        "plan",
        "birth_year",
        "interests",
    )
)

derived = (
    prepared
    .select(
        "id",
        "username",
        "plan",
        "birth_year",
        F.when(F.col("plan").isin("student", "premium"), "group_a").when(F.col("plan") == "free", "group_b").otherwise("other").alias("plan_group"),
    )
)

stats = (
    derived
    .groupBy("plan_group", "plan")
    .agg(
        F.array_sort(F.collect_set("plan")).alias("plan_list"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.upper(F.trim(F.col("username"))).like("H%"), 1)).alias("n_upper_trim_username_like_h"),
        F.countDistinct("plan_group").alias("distinct_plan_groups"),
        F.count(F.when(F.upper(F.trim(F.col("username"))).like("A%"), 1)).alias("n_upper_trim_username_like_a"),
        F.count(F.when(F.col("username").isNotNull(), 1)).alias("n_username_is_not_null"),
    )
)

result = (
    stats
    .filter(F.col("plan").isNotNull())
    .select(
        "plan_group",
        "plan",
        "plan_list",
        "row_count",
        "n_upper_trim_username_like_h",
        "distinct_plan_groups",
        "n_username_is_not_null",
        (F.col("n_username_is_not_null") + F.col("row_count")).alias("n_username_is_not_null_plus_row_count"),
    )
    .orderBy(F.col("plan").desc(), "plan_group")
    .limit(10)
)
