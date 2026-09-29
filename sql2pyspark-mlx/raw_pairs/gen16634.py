from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("city", "country").orderBy("days_since_signup_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("status").orderBy("next_days_since_signup_date", "id")
w4 = Window.partitionBy("city").orderBy("order_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-03-14").cast("date")) & F.col("segment").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
    )
)

enriched = (
    filtered
    .filter(F.col("credit_limit") > 14500)
    .select(
        "id",
        "city",
        "segment",
        "is_active",
        "age",
        "days_since_signup_date",
        F.lead("age").over(w_recent).alias("next_age"),
        F.lag("days_since_signup_date").over(w_recent).alias("prev_days_since_signup_date"),
        F.lead("days_since_signup_date").over(w_recent).alias("next_days_since_signup_date"),
        F.row_number().over(w_recent).alias("event_seq"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.amount").alias("order_amount"))
)

metrics = (
    with_latest
    .select(
        "id",
        "city",
        "is_active",
        "age",
        "next_age",
        "status",
        F.lead("prev_days_since_signup_date", 1).over(w3).alias("next_prev_days_since_signup_date"),
        F.first("city", ignorenulls=True).over(w4).alias("first_known_city"),
    )
)

result = (
    metrics
    .withColumn("age_tier", F.when(F.col("age") >= 64, "PREMIUM").otherwise("STANDARD"))
    .rollup("age_tier", "status")
    .agg(
        F.count_if(F.col("status") == "COMPLETED").alias("completed_events"),
        F.count(F.when(F.col("status") == "PENDING", 1)).alias("pending_count"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("next_prev_days_since_signup_date"))).alias("returned_total_next_prev_days_since_signup_date"),
        F.round(F.sum("next_prev_days_since_signup_date") / F.nullif(F.count(F.when(F.col("city") == "New York", 1)), F.lit(0)), 2).alias("next_prev_days_since_signup_date_per_new_york"),
        F.count_if(F.col("status") == "COMPLETED").alias("completed_events_2"),
        F.grouping_id().alias("grouping_id"),
    )
    .orderBy(F.col("status").desc_nulls_first())
)
