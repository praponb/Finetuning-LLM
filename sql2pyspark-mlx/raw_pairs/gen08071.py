from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy(F.col("start_date").asc_nulls_first(), "id")
w_grp = Window.partitionBy("status", "plan")
w3 = Window.partitionBy("customer_id")
w_hist = Window.partitionBy(F.month("start_date"), "city")
w_hist_2 = Window.partitionBy("city").orderBy(F.col("monthly_fee").desc_nulls_last())
w6 = Window.partitionBy("customer_id", "city").orderBy(F.col("monthly_fee").desc_nulls_first())
w7 = Window.partitionBy(F.year("start_date"), "customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())

deduped = (
    df_subscriptions
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    deduped
    .select("customer_id", "id")
    .intersectAll(
        df_subscriptions
        .filter(F.col("monthly_fee") < 120)
        .select("customer_id", "id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    kept_rows
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "latest_rank",
        F.sum("monthly_fee").over(w_grp).alias("status_sum_monthly_fee"),
        F.stddev_samp("monthly_fee").over(w3).alias("customer_id_stddev_samp_monthly_fee"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.start_date",
        "e.monthly_fee",
        F.col("b.age"),
        F.col("b.city"),
    )
)

metrics_subscriptions = (
    with_dims
    .filter(F.col("monthly_fee") < 90)
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        "city",
        F.min("age").over(w_hist).alias("start_date_month_min_age"),
        F.rank().over(w_hist_2).alias("monthly_fee_rank"),
        F.round((F.col("age") / F.sum("age").over(w3)), 4).alias("age_share"),
        F.rank().over(w6).alias("monthly_fee_rank_2"),
        F.rank().over(w7).alias("monthly_fee_rank_3"),
    )
)

result = (
    metrics_subscriptions
    .alias("src")
    .join(df_customers.alias("dim"), F.col("src.customer_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.city").alias("customer_city"),
        F.col("src.city").alias("subscription_city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("src.age_share")).alias("avg_age_share"),
        F.sum(F.col("src.monthly_fee_rank_3")).alias("total_monthly_fee_rank_3"),
    )
    .orderBy(F.col("subscription_city").asc_nulls_first())
)
