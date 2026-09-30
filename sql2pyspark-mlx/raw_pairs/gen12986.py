from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_filled").asc_nulls_last())

valid = (
    df_subscriptions
    .filter((F.col("start_date") >= F.lit("2023-03-04").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.coalesce("monthly_fee", F.lit(0)).alias("monthly_fee_filled"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id").eqNullSafe(F.col("b.id")), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.end_date",
        "e.monthly_fee_filled",
        F.col("b.city"),
    )
)

ranked = (
    with_dims
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

with_dims_subscriptions = (
    ranked
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.city").alias("customer_city"), F.col("d.name"))
)

attributed = (
    with_dims_subscriptions
    .alias("a")
    .join(df_subscriptions.alias("sub"), (F.col("a.customer_id") == F.col("sub.customer_id")) & (F.col("a.end_date") >= F.col("sub.start_date")) & (F.col("a.end_date") <= F.date_add(F.col("sub.end_date"), 10)), "inner")
    .select(
        "a.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

result = (
    attributed
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "inner")
    .cube(
        F.col("c.segment").alias("segment"),
        F.col("c.country").alias("country"),
        F.col("e.plan").alias("plan"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min(F.col("e.subscription_id")).alias("min_subscription_id"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("cnt").desc())
)
