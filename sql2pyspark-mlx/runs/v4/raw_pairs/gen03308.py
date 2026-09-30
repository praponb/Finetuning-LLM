from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id")
w5 = Window.partitionBy("city").orderBy(F.col("running_monthly_fee_p50").desc())

enriched = (
    df_subscriptions
    .withColumn("first_known_monthly_fee", F.first("monthly_fee", ignorenulls=True).over(w))
    .withColumn("running_monthly_fee", F.sum("monthly_fee").over(w2))
)

metrics = (
    enriched
    .select(
        "*",
        F.last("monthly_fee", ignorenulls=True).over(w3).alias("last_known_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w4)), 4).alias("monthly_fee_share"),
    )
)

with_dims = (
    metrics
    .alias("src")
    .join(df_customers.alias("d"), F.col("src.customer_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.city"))
)

matched_keys = (
    with_dims
    .select("plan")
    .subtract(
        df_subscriptions
        .filter(F.year("end_date") == 2025)
        .select("plan")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("first_known_monthly_fee") <= 80)
    .select(
        "*",
        F.date_format("start_date", "yyyy-MM-dd").alias("start_date_label"),
        F.add_months("end_date", 12).alias("end_date_plus_months"),
    )
)

with_dims_subscriptions = (
    base
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.signup_date"))
)

enriched_subscriptions = (
    with_dims_subscriptions
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.city").alias("customer_city"),
        F.col("d.signup_date").alias("customer_signup_date"),
    )
)

result = (
    enriched_subscriptions
    .groupBy("city", F.date_trunc("YEAR", "end_date_plus_months").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("customer_city") == "New York", F.col("monthly_fee_share")).otherwise(0)).alias("new_york_monthly_fee_share"),
        F.percentile_approx("running_monthly_fee", 0.5).alias("running_monthly_fee_p50"),
        F.size(F.collect_set("customer_city")).alias("num_customer_cities"),
    )
    .withColumn("running_monthly_fee_p50_percent_rank", F.percent_rank().over(w5))
)
