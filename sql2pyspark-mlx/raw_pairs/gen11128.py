from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("segment", "country").orderBy("age", "id")
w2 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w5 = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_first())

cleaned = (
    df_customers
    .filter(F.col("age").isNotNull())
    .select(
        "*",
        F.lower("segment").alias("segment_lower"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.lead("credit_limit").over(w_recent).alias("next_credit_limit"),
        F.lag("age").over(w_recent).alias("prev_age"),
        F.row_number().over(w_recent).alias("row_seq"),
        F.first("credit_limit", ignorenulls=True).over(w2).alias("first_known_credit_limit"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.country",
        "s.city",
        "s.segment",
        "s.is_active",
        "s.age",
        "s.segment_lower",
        "s.segment_clean",
        "s.next_credit_limit",
        "s.row_seq",
        F.col("c.amount"),
    )
)

with_last_customers = (
    with_last
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.browser").alias("session_browser"),
        F.col("c.country").alias("session_country"),
    )
)

ranked = (
    with_last_customers
    .withColumn("latest_rank", F.rank().over(w5))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "name",
        "country",
        "city",
        "is_active",
        "age",
        "segment_lower",
        "segment_clean",
        "amount",
        "session_country",
    )
)

result = (
    ranked
    .rollup(
        F.when(F.col("age") >= 61, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
    )
    .agg(
        F.sum(F.when(F.col("country") == "US", F.col("age"))).alias("us_total_age"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.round(F.sum("age") / F.nullif(F.count(F.when(F.col("country") == "TH", 1)), F.lit(0)), 2).alias("age_per_th"),
        F.count(F.when(F.col("session_country") == "DE", 1)).alias("de_rows"),
        F.avg("amount").alias("avg_amount"),
    )
    .orderBy(F.col("avg_amount").asc_nulls_first())
)
