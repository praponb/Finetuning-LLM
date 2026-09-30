from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city", "country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment")
w6 = Window.partitionBy("segment").orderBy(F.col("us_total_running_credit_limit").desc())

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-08-25").cast("date")) & F.col("city").isNotNull() & (F.upper(F.trim("email")) == "CAROL@COMPANY.CO"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.date_format("signup_date", "yyyy-MM-dd").alias("signup_date_label"),
    )
)

with_windows = (
    valid
    .withColumn("running_credit_limit", F.sum("credit_limit").over(w_running))
    .withColumn("first_known_credit_limit", F.first("credit_limit", ignorenulls=True).over(w2))
    .withColumn("first_known_age", F.first("age", ignorenulls=True).over(w_hist))
    .withColumn("last_known_credit_limit", F.last("credit_limit", ignorenulls=True).over(w_ordered))
)

matched_keys = (
    with_windows
    .select("id", "segment")
    .subtract(
        df_customers
        .filter(F.col("city") != "Tokyo")
        .select("id", "segment")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.status") == "EXPIRED"), "left_anti")
)

eligible_customers = (
    eligible
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") >= F.col("s.signup_date")), "left_anti")
)

not_flagged = (
    eligible_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.status").isin(["SHIPPED", "PENDING"])), "left_anti")
)

scored = (
    not_flagged
    .withColumn("segment_min_running_credit_limit", F.min("running_credit_limit").over(w5))
    .withColumn("segment_min_last_known_credit_limit", F.min("last_known_credit_limit").over(w5))
)

daily = (
    scored
    .groupBy(
        "segment",
        F.when(F.col("age") >= 73, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "US", F.col("running_credit_limit"))).alias("us_total_running_credit_limit"),
        F.count("*").alias("event_count"),
        F.count_if(F.col("country") == "DE").alias("de_events"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.count("*").alias("row_count"),
    )
    .withColumn("us_total_running_credit_limit_dense_rank", F.dense_rank().over(w6))
)

result = (
    daily
    .select(
        "segment",
        "age_tier",
        "us_total_running_credit_limit",
        "de_events",
        "distinct_ids",
        "cnt",
        "row_count",
        "us_total_running_credit_limit_dense_rank",
        F.when(F.col("segment").rlike("^(Corporate|SMB)$"), "PRIORITY").when(F.col("segment").rlike("^(Consumer)$"), "GROUP_B").otherwise("CORE").alias("segment_zone"),
    )
)
