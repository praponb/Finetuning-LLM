from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy("age", "id")
w2 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("credit_limit", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("country").orderBy("credit_limit_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("segment").orderBy("account_opened_date", "id")
w7 = Window.partitionBy("country").orderBy("running_sum_credit_limit_avg_6_prior", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w9 = Window.partitionBy("device")
w10 = Window.partitionBy("device").orderBy("prev_age", "id")
w11 = Window.partitionBy(F.month("started_at"), "device").orderBy("next_cumulative_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scored = (
    df_customers
    .select(
        "id",
        "country",
        "segment",
        "age",
        F.lag("age", 1).over(w).alias("prev_age"),
        F.sum("age").over(w2).alias("cumulative_age"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_6_prior"),
    )
)

with_latest = (
    scored
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.opened_date").alias("account_opened_date"),
        F.col("c.balance"),
    )
)

scored_customers = (
    with_latest
    .select(
        "id",
        "country",
        "prev_age",
        "account_opened_date",
        "balance",
        F.count("credit_limit_avg_6_prior").over(w5).alias("running_count_credit_limit_avg_6_prior"),
        F.avg("credit_limit_avg_6_prior").over(w5).alias("running_avg_credit_limit_avg_6_prior"),
        F.sum("credit_limit_avg_6_prior").over(w5).alias("running_sum_credit_limit_avg_6_prior"),
        F.lead("cumulative_age", 2).over(w6).alias("next_cumulative_age"),
    )
)

enriched = (
    scored_customers
    .select(
        "*",
        F.min("next_cumulative_age").over(w7).alias("last4_min_next_cumulative_age"),
        F.sum("next_cumulative_age").over(w7).alias("last4_sum_next_cumulative_age"),
        F.max("next_cumulative_age").over(w7).alias("last4_max_next_cumulative_age"),
        F.avg("next_cumulative_age").over(w7).alias("last4_avg_next_cumulative_age"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.account_opened_date")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.prev_age",
        "s.account_opened_date",
        "s.running_count_credit_limit_avg_6_prior",
        "s.running_sum_credit_limit_avg_6_prior",
        "s.next_cumulative_age",
        "s.last4_min_next_cumulative_age",
        F.col("c.started_at"),
        F.col("c.device"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "account_opened_date",
        "running_count_credit_limit_avg_6_prior",
        "running_sum_credit_limit_avg_6_prior",
        "next_cumulative_age",
        "last4_min_next_cumulative_age",
        "started_at",
        F.round((F.col("running_sum_credit_limit_avg_6_prior") / F.sum("running_sum_credit_limit_avg_6_prior").over(w9)), 4).alias("running_sum_credit_limit_avg_6_prior_share"),
        F.row_number().over(w10).alias("seq_no"),
        F.sum("next_cumulative_age").over(w11).alias("cumulative_next_cumulative_age"),
    )
)
