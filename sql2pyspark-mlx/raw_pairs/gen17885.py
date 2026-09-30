from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w2 = Window.orderBy(F.col("credit_limit").desc(), "id")
w3 = Window.partitionBy("segment").orderBy("credit_limit", "id")

base = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "credit_limit",
        F.upper("city").alias("city_upper"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

with_latest = (
    base
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"))
)

scored = (
    with_latest
    .filter(~F.col("status").isin(["OPEN", "FROZEN"]))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("next_credit_limit", F.lead("credit_limit", 1).over(w3))
)

not_flagged = (
    scored
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.customer_id").isin([2, 29])), "left_semi")
)

filtered = (
    not_flagged
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "credit_limit",
        "city_clean",
        "status",
        "event_seq",
        "next_credit_limit",
        F.trim("city").alias("clean_city"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_filled"),
    )
)

with_latest_customers = (
    filtered
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.account_type"))
)

grouped = (
    with_latest_customers
    .groupBy(
        F.when(F.col("next_credit_limit") >= 17400, "LARGE").otherwise("SMALL").alias("next_credit_limit_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.percentile_approx("credit_limit", 0.95).alias("credit_limit_p95"),
        F.count(F.when(F.col("city") == "London", 1)).alias("london_count"),
        F.size(F.collect_set("status")).alias("num_status"),
    )
)

result = (
    grouped
    .select(
        "next_credit_limit_tier",
        "account_type_list",
        "credit_limit_p95",
        "london_count",
    )
    .orderBy(F.col("credit_limit_p95").desc())
)
