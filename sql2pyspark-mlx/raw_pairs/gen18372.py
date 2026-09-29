from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("device").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("web_event_page", "country").orderBy("subscription_id", "id", "subscription_id")
w5 = Window.orderBy("subscription_id", "id", "subscription_id")

eligible = (
    df_sessions
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.page").isin(["/home", "/checkout"])), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.page").alias("web_event_page"))
)

enriched = (
    latest_match
    .filter(F.col("customer_id") == 9)
    .select(
        "*",
        F.first("web_event_page", ignorenulls=True).over(w2).alias("first_known_web_event_page"),
        F.first("country", ignorenulls=True).over(w3).alias("first_known_country"),
    )
)

attributed = (
    enriched
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.started_at") >= F.col("sub.start_date")) & (F.col("src.started_at") <= F.date_add(F.col("sub.end_date"), 7)), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

scored = (
    attributed
    .select(
        "id",
        "browser",
        "web_event_page",
        "first_known_country",
        "subscription_id",
        "plan",
        "monthly_fee",
        F.ntile(10).over(w4).alias("subscription_id_bucket"),
        F.row_number().over(w5).alias("seq_no"),
    )
)

result = (
    scored
    .withColumn("subscription_id_bucket_tier", F.when(F.col("subscription_id_bucket") >= 8, "LARGE").otherwise("SMALL"))
    .rollup("subscription_id_bucket_tier", "plan")
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_count"),
        F.sort_array(F.collect_set("browser")).alias("browser_list"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.sum(F.when(F.col("web_event_page") == "/product", F.col("monthly_fee")).otherwise(0)).alias("product_monthly_fee"),
        F.sort_array(F.collect_set("browser")).alias("browser_list_2"),
        F.grouping_id().alias("grouping_level"),
    )
)
