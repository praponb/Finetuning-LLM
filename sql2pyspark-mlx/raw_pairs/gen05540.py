from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("age", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment").orderBy("signup_date", "id")
w4 = Window.partitionBy(F.year("signup_date"), "country").orderBy("signup_date", "id")
w_part = Window.partitionBy("country").orderBy("prev_credit_limit", "id")
w6 = Window.partitionBy("country", "city").orderBy(F.col("next_credit_limit").asc_nulls_first())
w_grp = Window.partitionBy("segment").orderBy("next_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())

with_windows = (
    df_customers
    .select(
        "*",
        F.avg("credit_limit").over(w).alias("credit_limit_avg_6"),
        F.last("age", ignorenulls=True).over(w2).alias("last_known_age"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.lead("last_known_age").over(w3).alias("next_last_known_age"),
        F.lag("credit_limit").over(w3).alias("prev_credit_limit"),
        F.lead("credit_limit").over(w3).alias("next_credit_limit"),
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.lag("credit_limit").over(w_part).alias("prev_credit_limit_2"),
        F.lag("age").over(w_part).alias("prev_age"),
        F.row_number().over(w_part).alias("event_seq_2"),
        F.dense_rank().over(w6).alias("next_credit_limit_dense_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("signup_date") > F.lit("2023-09-24").cast("date"))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        "last_known_age",
        "prev_credit_limit",
        "next_credit_limit",
        "seq_no",
        "event_seq",
        "prev_credit_limit_2",
        "event_seq_2",
        F.ceil("prev_credit_limit_2").alias("prev_credit_limit_2_whole"),
        F.initcap(F.trim("city")).alias("city_title"),
        F.coalesce("segment", F.lit("NONE")).alias("resolved_segment"),
    )
)

with_windows_customers = (
    cleaned
    .select(
        "id",
        "signup_date",
        "age",
        "last_known_age",
        "prev_credit_limit",
        "seq_no",
        "event_seq_2",
        "prev_credit_limit_2_whole",
        "city_title",
        "resolved_segment",
        F.avg("event_seq_2").over(w_grp).alias("running_avg_event_seq_2"),
        F.count("event_seq_2").over(w_grp).alias("running_count_event_seq_2"),
        F.max("event_seq_2").over(w_grp).alias("running_max_event_seq_2"),
    )
)

with_last = (
    with_windows_customers
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.last_known_age",
        "s.seq_no",
        "s.prev_credit_limit_2_whole",
        "s.city_title",
        "s.resolved_segment",
        "s.running_max_event_seq_2",
        F.col("c.status"),
        F.col("c.account_type").alias("account_account_type"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.country").isin(["UK", "JP"])), "left_anti")
)

pivoted = (
    kept
    .groupBy("status")
    .pivot("account_account_type", ["LOAN", "CREDIT"])
    .agg(F.max("prev_credit_limit_2_whole"))
    .select("status", F.col("LOAN").alias("loan"), F.col("CREDIT").alias("credit"))
)

result = (
    pivoted
    .select("status", "credit")
    .orderBy(F.col("status").desc_nulls_last())
)
