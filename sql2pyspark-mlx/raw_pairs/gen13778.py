from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country", "segment")
w_running = Window.orderBy("credit_limit", "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("segment").orderBy("running_age", "id")
w6 = Window.partitionBy("device").orderBy("credit_limit_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("running_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("credit_limit_share", "id").rowsBetween(-2, Window.currentRow)
w9 = Window.partitionBy("device").orderBy("running_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter(F.col("is_active") & (~F.col("city").eqNullSafe(F.lit("Singapore"))))
    .select(
        "*",
        F.regexp_replace("email", " ", "-").alias("email_slug"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("resolved_segment"),
    )
)

enriched = (
    base
    .withColumn("running_age", F.sum("age").over(w))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4))
    .withColumn("next_age", F.lead("age", 1, 0).over(w_running))
)

latest_match = (
    enriched
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.segment",
        "s.signup_date",
        "s.resolved_segment",
        "s.running_age",
        "s.credit_limit_share",
        F.col("c.device"),
    )
)

metrics = (
    latest_match
    .filter(F.col("running_age") >= 120)
    .withColumn("next_credit_limit_share", F.lead("credit_limit_share", 1).over(w5))
    .withColumn("first_known_segment", F.first("segment", ignorenulls=True).over(w6))
    .withColumn("first_known_device", F.first("device", ignorenulls=True).over(w7))
    .withColumn("running_age_avg_2", F.avg("running_age").over(w8))
    .withColumn("first_known_running_age", F.first("running_age", ignorenulls=True).over(w9))
)

result = (
    metrics
    .select(
        "id",
        "name",
        "resolved_segment",
        "running_age",
        "credit_limit_share",
        "next_credit_limit_share",
        "first_known_running_age",
    )
)
