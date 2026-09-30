from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("subscription_monthly_fee").desc())
w2 = Window.partitionBy("city").orderBy("end_date", "id", "subscription_id")

joined = (
    df_subscriptions
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.end_date",
        "e.monthly_fee",
        "e.status",
        F.col("b.city"),
        F.col("b.segment"),
        F.col("b.age"),
    )
)

matched = (
    joined
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.end_date") >= F.col("sub.start_date")) & (F.col("a.end_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

deduped = (
    matched
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "end_date", "city", "subscription_id")
)

deduped_subscriptions = (
    deduped
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

filtered = (
    deduped_subscriptions
    .filter(F.col("city") == "London")
    .select(
        "*",
        F.floor("subscription_id").alias("subscription_id_whole"),
        F.coalesce("city", F.lit("NONE")).alias("resolved_city"),
    )
)

remaining_keys = (
    filtered
    .select("id")
    .subtract(
        df_subscriptions
        .filter(F.col("monthly_fee").between(70, 80))
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select("city", "subscription_id", "subscription_id_whole", "resolved_city")
    .orderBy(F.col("subscription_id").desc(), "subscription_id_whole")
)
