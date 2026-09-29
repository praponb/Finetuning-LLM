from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter(F.col("city").isNotNull() & (~F.col("segment").eqNullSafe(F.lit("SMB"))))
    .select(
        "*",
        F.initcap(F.trim("city")).alias("city_title"),
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.start_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.segment",
        "s.is_active",
        "s.age",
        "s.city_title",
        F.col("c.start_date"),
    )
)

eligible = (
    with_last
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.channel").isin(["mobile", "partner", "store"])), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.monthly_fee").alias("subscription_monthly_fee"),
        F.col("c.plan"),
    )
)

result = (
    latest_match
    .rollup(
        "segment",
        F.when(F.col("subscription_monthly_fee") >= 212, "HIGH").otherwise("LOW").alias("subscription_monthly_fee_tier"),
    )
    .agg(
        F.count(F.when(F.col("plan") == "enterprise", 1)).alias("enterprise_rows"),
        F.bit_and("age").alias("age_bit_and"),
        F.max("age").alias("max_age"),
        F.min("age").alias("min_age"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
)
