from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("is_active_flag").desc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-08-30"))) & F.col("segment").isNotNull())
    .select(
        "id",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.col("is_active").cast("int").alias("is_active_flag"),
        F.concat("city", F.lit(" - "), "email").alias("city_email"),
    )
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_latest = (
    deduped
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.browser").alias("session_browser"))
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.country") == "JP"), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "email",
        "city",
        "signup_date",
        "age",
        "is_active_flag",
        "city_email",
        "rank_in_group",
    )
)
