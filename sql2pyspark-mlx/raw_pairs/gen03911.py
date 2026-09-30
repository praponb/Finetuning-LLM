from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country")
w2 = Window.orderBy(F.col("credit_limit_times_age").desc_nulls_last())
w3 = Window.partitionBy("city").orderBy(F.col("credit_limit_times_age").desc(), "id")

valid = (
    df_customers
    .filter(F.col("signup_date") > F.lit("2023-06-22").cast("date"))
    .select(
        "id",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        (F.col("credit_limit") * F.col("age")).alias("credit_limit_times_age"),
    )
)

with_windows = (
    valid
    .filter(F.col("city") != "Singapore")
    .select(
        "*",
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w)), 4).alias("credit_limit_share"),
        F.cume_dist().over(w2).alias("credit_limit_times_age_cume_dist"),
        F.ntile(4).over(w3).alias("credit_limit_times_age_bucket"),
    )
)

filtered = (
    with_windows
    .filter(~F.col("is_active"))
    .withColumn("credit_limit_rounded", F.round("credit_limit", 2))
)

eligible = (
    filtered
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") < F.col("s.signup_date")), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "email",
        "country",
        "city",
        "is_active",
        "credit_limit",
        "credit_limit_times_age_cume_dist",
        "credit_limit_rounded",
    )
    .orderBy(F.col("credit_limit").desc_nulls_first())
)
