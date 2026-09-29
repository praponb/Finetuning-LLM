from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("age", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("country")
w4 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("city", "country").orderBy(F.col("age_share").desc(), "id")
w7 = Window.orderBy(F.col("age").asc_nulls_first())

eligible = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.plan") != "standard"), "left_anti")
)

cleaned = (
    eligible
    .filter((F.col("signup_date") > F.lit("2024-04-06").cast("date")) & F.col("country").isin(["TH", "DE"]))
    .withColumn("segment_lower", F.lower("segment"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.sum("credit_limit").over(w).alias("running_credit_limit"),
        F.avg("credit_limit").over(w2).alias("credit_limit_avg_2"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.round((F.col("running_credit_limit") / F.sum("running_credit_limit").over(w3)), 4).alias("running_credit_limit_share"),
        F.sum("age").over(w4).alias("running_age"),
        F.round((F.col("age") / F.sum("age").over(w5)), 4).alias("age_share"),
    )
)

eligible_customers = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") <= F.col("s.signup_date")), "left_anti")
)

enriched_customers = (
    eligible_customers
    .withColumn("age_share_bucket", F.ntile(5).over(w6))
    .withColumn("age_cume_dist", F.cume_dist().over(w7))
)

result = (
    enriched_customers
    .select(
        "id",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "segment_lower",
        F.when(F.col("country").rlike("^(TH|US)$"), "GROUP_B").when(F.col("country").rlike("^(SG|JP)$"), "EXTENDED").otherwise("CORE").alias("country_zone"),
    )
)
