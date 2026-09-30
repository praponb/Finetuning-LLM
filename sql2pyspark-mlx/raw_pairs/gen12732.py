from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(-2, -1)

joined = (
    df_subscriptions
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.name"))
)

with_windows = (
    joined
    .withColumn("last_known_monthly_fee", F.last("monthly_fee", ignorenulls=True).over(w_grp))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w2))
    .withColumn("monthly_fee_avg_2_prior", F.avg("monthly_fee").over(w3))
)

filtered = (
    with_windows
    .filter(F.col("start_date") >= F.to_date(F.lit("2023-05-10")))
    .select(
        "*",
        (F.col("monthly_fee_avg_2_prior") + F.col("last_known_monthly_fee")).alias("monthly_fee_avg_2_prior_plus_last_known_monthly_fee"),
        F.lower("status").alias("status_lower"),
    )
)

joined_subscriptions = (
    filtered
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .filter(F.col("b.credit_limit") >= 9500)
    .select("src.*", F.col("b.signup_date"))
)

result = (
    joined_subscriptions
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment").alias("segment"), F.col("e.plan").alias("plan"))
    .agg(
        F.count("*").alias("num_rows"),
        F.max(F.col("e.monthly_fee")).alias("max_monthly_fee"),
    )
    .orderBy(F.col("plan").desc_nulls_last())
)
