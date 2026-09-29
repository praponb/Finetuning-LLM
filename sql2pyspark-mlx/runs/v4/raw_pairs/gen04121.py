from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").asc_nulls_last(), "id")
w2 = Window.partitionBy("city", "segment").orderBy("credit_limit")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

deduped = (
    df_customers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

valid = (
    deduped
    .filter(F.col("segment").isNotNull())
    .select(
        "*",
        F.when(F.col("city").isin(["Chiang Mai", "Bangkok"]), "Core").otherwise("Other").alias("city_group"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") < F.col("s.signup_date")), "left_anti")
)

latest = (
    kept
    .withColumn("dedup_rank", F.rank().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

latest_match = (
    latest
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.browser"), F.col("c.started_at"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") < F.col("s.signup_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "city_group",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
