from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").desc_nulls_last())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

deduped = (
    df_customers
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "email", "city", "segment", "signup_date", "credit_limit")
)

with_latest = (
    deduped
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.monthly_fee").alias("subscription_monthly_fee"),
        F.col("c.end_date"),
    )
)

with_last = (
    with_latest
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"))
)

kept_keys = (
    with_last
    .select("id", "segment")
    .exceptAll(
        df_customers
        .filter(F.col("email").like("%@mail.co.th"))
        .select("id", "segment")
    )
)

reconciled = (
    with_last
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.browser") == "Chrome"), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id").isNotNull()), "left_anti")
)

result = (
    kept
    .select("id", "email", "city", "signup_date", "credit_limit", "status")
    .orderBy(F.col("id").asc_nulls_first())
)
