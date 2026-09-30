from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("signup_date", "id")
w3 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("running_credit_limit").desc_nulls_first())
w5 = Window.partitionBy(F.year("signup_date")).orderBy("next_name_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-02-06").cast("date"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
        F.when(F.col("city").isin(["Singapore", "Berlin"]), "Core").otherwise("Other").alias("city_group"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_clean"),
    )
)

base = (
    cleaned
    .filter(F.col("is_active"))
    .select(
        "*",
        F.length("name").alias("name_length"),
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

with_windows = (
    base
    .filter(F.col("signup_date").between("2024-06-01", "2024-12-01"))
    .select(
        "id",
        "name",
        "email",
        "signup_date",
        "is_active",
        "city_filled",
        "city_group",
        "segment_clean",
        "email_domain",
        "resolved_city",
        F.sum("credit_limit").over(w).alias("running_credit_limit"),
        F.lead("name_length", 1).over(w2).alias("next_name_length"),
    )
)

kept = (
    with_windows
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") <= F.col("s.signup_date")), "left_anti")
)

scored = (
    kept
    .select(
        "id",
        "name",
        "email",
        "signup_date",
        "city_group",
        "segment_clean",
        "email_domain",
        "resolved_city",
        "running_credit_limit",
        F.sum("running_credit_limit").over(w3).alias("running_running_credit_limit"),
        F.dense_rank().over(w4).alias("running_credit_limit_dense_rank"),
        F.sum("next_name_length").over(w5).alias("running_next_name_length"),
        F.last("next_name_length", ignorenulls=True).over(w6).alias("last_known_next_name_length"),
    )
)

not_flagged = (
    scored
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.status").isin(["CLOSED", "FROZEN"])), "left_semi")
)

grouped = (
    not_flagged
    .groupBy(
        F.when(F.col("last_known_next_name_length") >= 81, "LARGE").otherwise("SMALL").alias("last_known_next_name_length_tier"),
    )
    .agg(
        F.percentile_approx("running_next_name_length", [0.25, 0.5, 0.75]).alias("running_next_name_length_quartiles"),
        F.count("*").alias("cnt"),
        F.round(F.avg("running_running_credit_limit"), 2).alias("avg_running_running_credit_limit"),
    )
)

result = (
    grouped
    .select(
        "last_known_next_name_length_tier",
        "running_next_name_length_quartiles",
        "cnt",
        "avg_running_running_credit_limit",
    )
)
