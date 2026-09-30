from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city", "country").orderBy(F.col("days_since_signup_date").desc(), "id")
w3 = Window.orderBy("credit_limit", "id")
w4 = Window.partitionBy("country", "city").orderBy("credit_limit", "id")
w5 = Window.partitionBy("city", "country")
w6 = Window.partitionBy("city").orderBy(F.col("days_since_signup_date").desc(), "id")
w7 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter(F.col("segment").isNotNull() & (~F.col("city").eqNullSafe(F.lit("Bangkok"))))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "credit_limit",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
        F.round((F.col("age") * 1.1), 2).alias("adjusted_age"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.order_date").alias("order_order_date"))
)

latest = (
    with_last
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .select(
        "id",
        "name",
        "city",
        "signup_date",
        "days_since_signup_date",
        "adjusted_age",
        "dedup_rank",
        F.ntile(5).over(w3).alias("credit_limit_bucket"),
        F.lag("days_since_signup_date", 2).over(w4).alias("prev_days_since_signup_date"),
        F.min("adjusted_age").over(w5).alias("city_min_adjusted_age"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

with_last_customers = (
    ranked
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.country").alias("session_country"))
)

result = (
    with_last_customers
    .select(
        "id",
        "city",
        "signup_date",
        "days_since_signup_date",
        "credit_limit_bucket",
        "prev_days_since_signup_date",
        "city_min_adjusted_age",
        "row_num",
        "session_country",
    )
    .orderBy(
        F.col("prev_days_since_signup_date").desc_nulls_first(),
        "city_min_adjusted_age",
    )
)
