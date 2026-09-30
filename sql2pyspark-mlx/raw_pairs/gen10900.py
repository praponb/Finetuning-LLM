from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("signup_date"), "segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.year("signup_date"), "city").orderBy("credit_limit", "id")

valid = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select("*", F.regexp_replace("name", " ", "-").alias("name_slug"))
)

not_flagged = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.start_date") <= F.col("s.signup_date")), "left_semi")
)

with_windows = (
    not_flagged
    .select(
        "id",
        "name",
        "city",
        "is_active",
        "credit_limit",
        "age",
        F.first("credit_limit", ignorenulls=True).over(w).alias("first_known_credit_limit"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.monthly_fee") > 130), "left_anti")
)

result = (
    eligible
    .groupBy("city")
    .agg(
        F.percentile_approx("credit_limit", [0.25, 0.5, 0.75]).alias("credit_limit_quartiles"),
        F.percentile_approx("first_known_credit_limit", 0.5).alias("first_known_credit_limit_p50"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("age")).otherwise(0)).alias("singapore_age"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("credit_limit")).otherwise(0)).alias("bangkok_credit_limit"),
    )
    .orderBy("singapore_age")
)
