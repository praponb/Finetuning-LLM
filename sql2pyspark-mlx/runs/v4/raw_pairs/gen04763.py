from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.start_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("subscription_plan").orderBy("age", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("segment").orderBy(F.col("monthly_fee").desc(), "id")
w4 = Window.orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("least_is_active_flag_age", "id")
w6 = Window.orderBy("is_active_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2024-03-24"))) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "credit_limit",
        "age",
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)

cleaned = (
    base
    .filter(F.col("name").like("Emm%"))
    .select(
        "id",
        "name",
        "email",
        "segment",
        "credit_limit",
        "age",
        "is_active_flag",
        F.least("is_active_flag", "age").alias("least_is_active_flag_age"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("resolved_segment"),
    )
)

with_last = (
    cleaned
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.monthly_fee"), F.col("c.plan").alias("subscription_plan"))
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.status") == "PAUSED"), "left_anti")
)

scored = (
    not_flagged
    .withColumn("credit_limit_avg_2_prior", F.avg("credit_limit").over(w2))
    .withColumn("seq_no", F.row_number().over(w3))
)

result = (
    scored
    .select(
        "*",
        F.first("age", ignorenulls=True).over(w4).alias("first_known_age"),
        F.lead("credit_limit_avg_2_prior", 2).over(w5).alias("next_credit_limit_avg_2_prior"),
        F.sum("monthly_fee").over(w6).alias("running_monthly_fee"),
    )
)
