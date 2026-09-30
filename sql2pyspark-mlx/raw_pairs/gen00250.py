from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("city", "segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-02-28"))) & (F.col("country") == "JP"))
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
)

metrics = (
    cleaned
    .select(
        "*",
        F.lag("age").over(w).alias("prev_age"),
        F.lead("age").over(w).alias("next_age"),
        F.lead("credit_limit").over(w).alias("next_credit_limit"),
        F.lag("credit_limit").over(w).alias("prev_credit_limit"),
        F.row_number().over(w).alias("row_seq"),
    )
)

latest_match = (
    metrics
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.started_at").alias("session_started_at"),
        F.col("c.country").alias("session_country"),
    )
)

enriched = (
    latest_match
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "credit_limit",
        "age",
        "is_active_label",
        "prev_credit_limit",
        "session_country",
        F.avg("next_credit_limit").over(w3).alias("running_avg_next_credit_limit"),
        F.max("next_credit_limit").over(w3).alias("running_max_next_credit_limit"),
        F.min("next_credit_limit").over(w3).alias("running_min_next_credit_limit"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.shipping_fee") >= 31), "left_anti")
)

daily = (
    eligible
    .groupBy(F.date_trunc("QUARTER", "signup_date").alias("quarter_start"))
    .agg(
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("credit_limit_per_de"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("country") == "SG", 1)), F.lit(0)), 2).alias("credit_limit_per_sg"),
    )
)

result = (
    daily
    .select("quarter_start", "credit_limit_per_de", "credit_limit_per_sg")
    .orderBy(F.col("credit_limit_per_sg").asc_nulls_last())
)
