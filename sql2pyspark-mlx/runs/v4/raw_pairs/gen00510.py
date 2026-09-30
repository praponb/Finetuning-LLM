from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("end_date").asc_nulls_last(), "id", "subscription_id")

attributed = (
    df_subscriptions
    .alias("s")
    .join(df_subscriptions.alias("sub"), (F.col("s.customer_id") == F.col("sub.customer_id")) & F.col("s.end_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "left")
    .select(
        "s.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

ranked = (
    attributed
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_dims = (
    ranked
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.name"), F.col("d.signup_date"))
)

totals = (
    with_dims
    .groupBy(F.date_trunc("QUARTER", "signup_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("row_count"),
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
)

result = (
    totals
    .select("quarter_start", "row_count", "num_plans")
    .orderBy(F.col("row_count").desc())
)
