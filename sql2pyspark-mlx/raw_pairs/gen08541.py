from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("end_date", "id")

enriched = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .filter(F.col("d.country") != "JP")
    .select("s.*", F.col("d.city"), F.col("d.age"), F.col("d.segment"))
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "start_date", "end_date", "status", "age", "segment")
)

joined = (
    latest
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .select(
        "a.id",
        "a.customer_id",
        "a.end_date",
        "a.age",
        F.col("c.name"),
        F.col("c.signup_date"),
        F.col("c.country"),
    )
)

joined_subscriptions = (
    joined
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.signup_date").alias("customer_signup_date"))
)

enriched_subscriptions = (
    joined_subscriptions
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.segment"), F.col("d.age").alias("customer_age"))
)

matched = (
    enriched_subscriptions
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.customer_signup_date") >= F.col("sub.start_date")) & (F.col("a.customer_signup_date") <= F.date_add(F.col("sub.end_date"), 7)), "left")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

totals = (
    matched
    .withColumn("year_start", F.date_trunc("YEAR", "customer_signup_date"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "end_date"))
    .groupingSets(
        [["year_start", "quarter_start"], ["year_start"], []],
        "year_start",
        "quarter_start",
    )
    .agg(
        F.avg(F.when(F.col("plan") == "premium", F.col("customer_age"))).alias("premium_avg_customer_age"),
        F.count_if(F.col("plan") == "standard").alias("standard_events"),
    )
)

result = totals.select("premium_avg_customer_age", "standard_events")
