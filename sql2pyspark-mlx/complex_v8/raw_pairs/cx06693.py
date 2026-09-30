from pyspark.sql import functions as F

base_listeners = (
    df_listeners
    .filter((F.year(F.col("signup_date")) == 2025) & (F.upper(F.trim(F.col("username"))).like("C%")))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.date_trunc("quarter", F.col("signup_date")).alias("signup_date_quarter"),
        F.abs(F.col("birth_year") - 1970).alias("birth_year_distance"),
        F.when(F.col("plan").isin("family", "free"), "group_a").when(F.col("plan") == "student", "group_b").otherwise("other").alias("plan_group"),
    )
)

enriched = (
    base_listeners
    .filter(F.col("birth_year").between(1966, 1981))
    .select(
        "*",
        F.coalesce(F.col("birth_year"), F.lit(0)).alias("birth_year_filled"),
        F.concat_ws(" / ", F.col("plan"), F.col("username")).alias("plan_username_label"),
    )
)

exploded = (
    enriched
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "signup_date_quarter",
        "birth_year_distance",
        "plan_group",
        "birth_year_filled",
        "plan_username_label",
        F.posexplode("interests").alias("interest_pos", "interest"),
    )
)

screened = (
    exploded
    .crossJoin(exploded.agg(F.avg("interest_pos").alias("overall_avg_interest_pos")))
    .filter(F.col("interest_pos") > F.col("overall_avg_interest_pos"))
    .drop("overall_avg_interest_pos")
)

result = (
    screened
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "signup_date_quarter",
        "birth_year_distance",
        "plan_group",
        "interest_pos",
        "interest",
        F.abs(F.col("birth_year") - 1975).alias("birth_year_distance_2"),
    )
    .orderBy("id", "country")
)
