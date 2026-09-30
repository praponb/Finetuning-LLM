from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc(), "id")

deduped = (
    df_customers
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") >= F.col("s.signup_date")), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.end_date") <= F.col("s.signup_date")), "left_anti")
)

eligible_customers = (
    eligible
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") < F.col("s.signup_date")), "left_anti")
)

result = (
    eligible_customers
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("activity_month"))
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.percentile_approx("age", [0.25, 0.5, 0.75]).alias("age_quartiles"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.stddev_samp("age").alias("age_stddev"),
    )
    .filter(F.col("age_stddev") < 0)
    .orderBy(F.col("age_stddev").desc_nulls_last())
)
