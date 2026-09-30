from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter(F.col("signup_date").between("2024-06-01", "2025-09-01") & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.coalesce("segment", F.lit("OTHER")).alias("segment_filled"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.status") == "CANCELLED"), "left_anti")
)

valid_customers = (
    eligible
    .filter((F.col("signup_date") >= F.lit("2023-05-15").cast("date")) & F.col("is_active"))
    .select(
        "id",
        "name",
        "email",
        "credit_limit",
        "age",
        "is_active_label",
        "segment_filled",
        F.substring("email", 1, 4).alias("email_prefix"),
    )
)

with_windows = (
    valid_customers
    .filter(F.col("age") != 52)
    .select(
        "*",
        F.last("age", ignorenulls=True).over(w_part).alias("last_known_age"),
        F.sum("age").over(w_part).alias("cumulative_age"),
    )
)

eligible_customers = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.customer_id").isin([15, 21])), "left_semi")
)

valid_customers_2 = (
    eligible_customers
    .filter(F.col("age").between(39, 44))
    .select("*", F.regexp_replace("email", " ", "-").alias("email_slug"))
)

report = (
    valid_customers_2
    .select(
        "id",
        "name",
        "email",
        "credit_limit",
        "age",
        "segment_filled",
        "email_prefix",
        "last_known_age",
        "email_slug",
    )
    .orderBy("credit_limit", F.col("age").desc())
)

result = (
    report
    .select(
        "id",
        "email",
        "credit_limit",
        "segment_filled",
        "email_prefix",
        "last_known_age",
    )
    .orderBy("credit_limit")
)
