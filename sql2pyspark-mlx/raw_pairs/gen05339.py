from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("account_status", "city").orderBy(F.col("credit_limit").desc(), "id")
w3 = Window.partitionBy("account_status").orderBy(F.col("account_opened_date_2").desc_nulls_last(), "id")
w4 = Window.partitionBy("city").orderBy(F.col("tokyo_events").desc())

cleaned = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "is_active",
        "credit_limit",
        "age",
        F.initcap(F.trim("city")).alias("city_title"),
        F.regexp_extract("email", "@(.+)$", 1).alias("email_domain"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.opened_date").alias("account_opened_date"),
        F.col("c.status").alias("account_status"),
    )
)

with_last = (
    latest_match
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.city",
        "s.is_active",
        "s.credit_limit",
        "s.account_status",
        F.col("c.opened_date").alias("account_opened_date_2"),
        F.col("c.account_type").alias("account_account_type"),
    )
)

ranked = (
    with_last
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "city", "credit_limit")
)

eligible = (
    latest
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.status") == "CANCELLED"), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.country").isNull()), "left_semi")
)

result = (
    not_flagged
    .groupBy(
        "city",
        F.when(F.col("credit_limit") >= 17600, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
    )
    .agg(
        F.sum(F.when(F.col("city") == "Berlin", F.col("credit_limit"))).alias("berlin_total_credit_limit"),
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("credit_limit_per_new_york"),
    )
    .withColumn("tokyo_events_percent_rank", F.percent_rank().over(w4))
    .orderBy(F.col("berlin_total_credit_limit").desc())
)
