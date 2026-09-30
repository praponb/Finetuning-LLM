from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(-6, Window.currentRow)
w_running = Window.partitionBy("segment", "country").orderBy("credit_limit", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("country").orderBy("last7_count_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w6 = Window.partitionBy("country").orderBy("balance", "id")
w7 = Window.orderBy("age_avg_3_prior", "id")
w_grp = Window.partitionBy("session_browser").orderBy("last7_min_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("account_type").orderBy("running_sum_last7_avg_age", "id")

metrics = (
    df_customers
    .withColumn("last7_avg_age", F.avg("age").over(w_recent))
    .withColumn("last7_min_age", F.min("age").over(w_recent))
    .withColumn("last7_count_age", F.count("age").over(w_recent))
    .withColumn("last7_max_age", F.max("age").over(w_recent))
    .withColumn("age_avg_3_prior", F.avg("age").over(w_running))
)

enriched = (
    metrics
    .filter(F.col("age").between(34, 66))
    .select(
        "id",
        "name",
        "country",
        "city",
        "signup_date",
        "age",
        "last7_min_age",
        "last7_count_age",
        "last7_max_age",
        "age_avg_3_prior",
        F.avg("last7_avg_age").over(w3).alias("running_avg_last7_avg_age"),
        F.min("last7_avg_age").over(w3).alias("running_min_last7_avg_age"),
        F.max("last7_avg_age").over(w3).alias("running_max_last7_avg_age"),
        F.sum("last7_avg_age").over(w3).alias("running_sum_last7_avg_age"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "left_semi")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.last7_min_age",
        "s.last7_max_age",
        "s.age_avg_3_prior",
        "s.running_sum_last7_avg_age",
        F.col("c.balance"),
        F.col("c.account_type"),
    )
)

latest_match = (
    with_last
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.device").alias("session_device"),
        F.col("c.browser").alias("session_browser"),
    )
)

not_flagged_customers = (
    latest_match
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.account_type").isin(["SAVINGS", "CHECKING"])), "left_semi")
)

metrics_customers = (
    not_flagged_customers
    .select(
        "id",
        "last7_min_age",
        "running_sum_last7_avg_age",
        "session_device",
        F.lag("last7_min_age", 1, 0).over(w6).alias("prev_last7_min_age"),
        F.lead("last7_max_age", 1).over(w7).alias("next_last7_max_age"),
        F.sum("last7_max_age").over(w_grp).alias("running_last7_max_age"),
        F.row_number().over(w9).alias("seq_no"),
    )
)

result = (
    metrics_customers
    .groupBy(
        F.when(F.col("running_sum_last7_avg_age") >= 303, "LARGE").otherwise("SMALL").alias("running_sum_last7_avg_age_tier"),
        F.when(F.col("last7_min_age") >= 59, "PREMIUM").otherwise("STANDARD").alias("last7_min_age_tier"),
        "session_device",
    )
    .agg(
        F.countDistinct("session_device").alias("distinct_session_devices"),
        F.min("running_last7_max_age").alias("min_running_last7_max_age"),
        F.sort_array(F.collect_set("session_device")).alias("session_device_list"),
    )
    .filter(F.col("min_running_last7_max_age") < 1000000)
    .orderBy(F.col("distinct_session_devices").asc_nulls_last())
)
