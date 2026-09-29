from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.date_trunc("YEAR", "signup_date_minus_86d"), "segment").orderBy("credit_limit", "id")
w_ordered = Window.partitionBy(F.month("signup_date"), "segment").orderBy("signup_date", "id").rowsBetween(-6, Window.currentRow)

latest_match = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee"), F.col("c.discount").alias("order_discount"))
)

eligible = (
    latest_match
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.browser") == "Chrome"), "left_semi")
)

valid = (
    eligible
    .filter(F.col("city").eqNullSafe(F.lit("New York")))
    .select(
        "id",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "shipping_fee",
        "order_discount",
        F.date_sub("signup_date", 86).alias("signup_date_minus_86d"),
    )
)

with_windows = (
    valid
    .filter(~F.col("segment").isin(["Corporate", "SMB"]))
    .select(
        "id",
        "email",
        "signup_date",
        "is_active",
        "shipping_fee",
        "order_discount",
        "signup_date_minus_86d",
        F.lead("age", 1).over(w2).alias("next_age"),
        F.avg("order_discount").over(w_ordered).alias("order_discount_avg_6"),
    )
)

eligible_customers = (
    with_windows
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.shipping_fee") < 14), "left_anti")
)

result = (
    eligible_customers
    .select(
        "id",
        "email",
        "signup_date",
        "shipping_fee",
        "order_discount",
        "signup_date_minus_86d",
        "next_age",
        "order_discount_avg_6",
    )
)
