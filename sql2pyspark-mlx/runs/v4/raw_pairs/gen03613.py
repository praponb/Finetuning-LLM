from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city").orderBy(F.col("age").asc_nulls_last())
w3 = Window.partitionBy("city", "segment").orderBy("credit_limit", "id").rowsBetween(-3, -1)
w4 = Window.partitionBy("segment")
w5 = Window.partitionBy("country", "segment").orderBy("age", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("segment").orderBy(F.col("segment_stddev_samp_credit_limit").desc_nulls_last())
w7 = Window.partitionBy("city", "country").orderBy(F.col("segment_stddev_samp_credit_limit_percent_rank").desc(), "id")
w8 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

kept = (
    df_customers
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") < F.col("s.signup_date")), "left_semi")
)

scored = (
    kept
    .select(
        "*",
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.cume_dist().over(w2).alias("age_cume_dist"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_3_prior"),
        F.stddev_samp("credit_limit").over(w4).alias("segment_stddev_samp_credit_limit"),
    )
)

scored_customers = (
    scored
    .select(
        "*",
        F.avg("age_cume_dist").over(w5).alias("age_cume_dist_avg_5"),
        F.round((F.col("segment_stddev_samp_credit_limit") / F.sum("segment_stddev_samp_credit_limit").over(w4)), 4).alias("segment_stddev_samp_credit_limit_share"),
        F.percent_rank().over(w6).alias("segment_stddev_samp_credit_limit_percent_rank"),
    )
)

ranked = (
    scored_customers
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w8))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.channel"))
)

valid = (
    with_latest
    .filter(F.col("age").isNull())
    .select("*", F.month("signup_date").alias("signup_date_month"))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") >= F.col("s.signup_date")), "left_anti")
)

result = (
    not_flagged
    .groupBy("channel")
    .pivot("country", ["TH", "JP"])
    .agg(F.sum("credit_limit_avg_3_prior"))
)
