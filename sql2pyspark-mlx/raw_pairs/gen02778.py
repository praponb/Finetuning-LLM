from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("order_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("segment").orderBy("session_started_at", "id")
w6 = Window.partitionBy("segment").orderBy("session_started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-05-29"))) & F.col("city").isNotNull())
    .select(
        "id",
        "email",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        F.concat_ws(",", "name", "country").alias("name_country_key"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.shipping_fee").alias("order_shipping_fee"),
        F.col("c.discount").alias("order_discount"),
    )
)

scored = (
    with_latest
    .filter(F.col("segment").isin(["Consumer", "Corporate", "SMB"]))
    .withColumn("running_min_credit_limit", F.min("credit_limit").over(w2))
    .withColumn("running_sum_credit_limit", F.sum("credit_limit").over(w2))
    .withColumn("running_count_credit_limit", F.count("credit_limit").over(w2))
    .withColumn("running_order_discount", F.sum("order_discount").over(w3))
)

eligible = (
    scored
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") >= F.col("s.signup_date")), "left_anti")
)

with_latest_customers = (
    eligible
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.browser").alias("session_browser"),
        F.col("c.started_at").alias("session_started_at"),
    )
)

flagged = (
    with_latest_customers
    .withColumn("is_new_burst", F.when(F.col("session_started_at") - F.lag("session_started_at").over(w5) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_customers = (
    flagged
    .select(
        "id",
        "city",
        "segment",
        "signup_date",
        "running_sum_credit_limit",
        "running_order_discount",
        "session_started_at",
        F.sum("is_new_burst").over(w6).alias("burst_id"),
    )
)

result = (
    sessions_customers
    .cube(
        F.when(F.col("running_sum_credit_limit") >= 83000, "HIGH").otherwise("LOW").alias("running_sum_credit_limit_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.min("session_started_at").alias("first_session_started_at"),
        F.max("session_started_at").alias("ended_at"),
    )
    .orderBy(F.col("running_sum_credit_limit_tier").desc())
)
