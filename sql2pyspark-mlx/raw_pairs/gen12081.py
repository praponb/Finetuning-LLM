from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter(F.col("segment").isNotNull() & (F.col("signup_date") >= "2025-05-01"))
    .withColumn("email_lower", F.lower("email"))
)

with_latest = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("subscription_status"), F.col("c.plan"))
)

filtered = (
    with_latest
    .filter((F.col("signup_date") > F.lit("2023-05-31").cast("date")) & F.col("subscription_status").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "subscription_status",
        "plan",
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)

matched_keys = (
    filtered
    .select("city", "country")
    .subtract(
        df_customers
        .filter(F.length("email") >= 5)
        .select("city", "country")
    )
)

verified = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

pivoted = (
    verified
    .groupBy("subscription_status")
    .pivot("segment", ["Consumer", "Corporate", "SMB"])
    .agg(F.sum("is_active_flag"))
)

result = (
    pivoted
    .select("subscription_status", "Consumer", "Corporate", "SMB")
    .orderBy(F.col("Consumer").desc())
)
