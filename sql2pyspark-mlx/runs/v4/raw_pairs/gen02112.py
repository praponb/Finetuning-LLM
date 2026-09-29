from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_last())
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_first())
w_grp = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc(), "id")
w4 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").asc_nulls_last())
w_running = Window.orderBy(F.col("age").desc_nulls_last())
w_running_2 = Window.partitionBy("customer_id")

enriched = (
    df_subscriptions
    .select(
        "*",
        F.cume_dist().over(w_hist).alias("monthly_fee_cume_dist"),
        F.cume_dist().over(w2).alias("monthly_fee_cume_dist_2"),
        F.ntile(10).over(w_grp).alias("monthly_fee_bucket"),
        F.cume_dist().over(w4).alias("monthly_fee_cume_dist_3"),
    )
)

enriched_subscriptions = (
    enriched
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.age"))
)

cleaned = (
    enriched_subscriptions
    .filter(F.col("monthly_fee_cume_dist_2") > 0.43)
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "monthly_fee",
        "monthly_fee_cume_dist_2",
        "age",
        F.substring("plan", 1, 4).alias("plan_prefix"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.percent_rank().over(w_running).alias("age_percent_rank"),
        F.sum("monthly_fee").over(w_running_2).alias("customer_id_sum_monthly_fee"),
    )
)

result = (
    scored
    .select(
        "id",
        "start_date",
        "end_date",
        "monthly_fee",
        "monthly_fee_cume_dist_2",
        "age",
        "customer_id_sum_monthly_fee",
    )
    .orderBy("age", "customer_id_sum_monthly_fee")
)
