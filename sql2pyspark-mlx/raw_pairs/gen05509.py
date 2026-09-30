from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(-6, -1)
w_grp = Window.partitionBy(F.year("signup_date"), "country").orderBy("credit_limit", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("segment", "city").orderBy("running_min_age_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy(F.col("running_min_age_avg_6").desc(), "id")

filtered = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.substring("city", 1, 3).alias("city_prefix"),
        F.upper(F.trim("name")).alias("name_normalized"),
        F.coalesce("city", F.lit("N/A")).alias("resolved_city"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.count("age").over(w).alias("running_count_age"),
        F.min("age").over(w).alias("running_min_age"),
        F.sum("age").over(w).alias("running_sum_age"),
        F.max("age").over(w).alias("running_max_age"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") >= F.col("s.signup_date")), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "name_normalized",
        "running_count_age",
        "running_sum_age",
        F.avg("credit_limit").over(w2).alias("credit_limit_avg_6_prior"),
        F.avg("running_min_age").over(w_grp).alias("running_min_age_avg_6"),
    )
)

valid = (
    enriched
    .filter(F.col("segment").eqNullSafe(F.lit("Corporate")))
    .select(
        "id",
        "name",
        "city",
        "segment",
        "signup_date",
        "name_normalized",
        "running_min_age_avg_6",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_clean"),
    )
)

enriched_customers = (
    valid
    .select(
        "id",
        "city",
        "name_normalized",
        "running_min_age_avg_6",
        F.sum("running_min_age_avg_6").over(w4).alias("cumulative_running_min_age_avg_6"),
        F.sum("running_min_age_avg_6").over(w5).alias("running_running_min_age_avg_6"),
        F.ntile(10).over(w6).alias("running_min_age_avg_6_bucket"),
    )
)

totals = (
    enriched_customers
    .groupBy("city")
    .agg(
        F.countDistinct("city").alias("distinct_cities"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count(F.when(F.col("city") == "Berlin", 1)).alias("berlin_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    totals
    .select(
        "city",
        "distinct_ids",
        F.when(F.col("city").rlike("^(Chiang Mai|New York)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy(F.col("distinct_ids").desc(), "city_zone")
)
