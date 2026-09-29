from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("age").desc_nulls_last())
w2 = Window.partitionBy("country").orderBy(F.col("age").desc_nulls_last())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.monthly_fee").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("country").orderBy("credit_limit", "id")
w5 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

latest = (
    df_customers
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
    )
)

ranked = (
    latest
    .withColumn("rn", F.rank().over(w2))
    .filter(F.col("rn") == 1)
)

with_last = (
    ranked
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w3))
    .filter(F.col("rn_2") == 1)
    .select(
        "s.id",
        "s.country",
        "s.is_active",
        "s.credit_limit",
        "s.age",
        "s.rn",
        F.col("c.end_date"),
    )
)

metrics = (
    with_last
    .select(
        "id",
        "credit_limit",
        "age",
        F.lag("age", 2).over(w4).alias("prev_age"),
        F.first("credit_limit", ignorenulls=True).over(w5).alias("first_known_credit_limit"),
    )
)

with_last_customers = (
    metrics
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.prev_age",
        "s.first_known_credit_limit",
        F.col("c.start_date"),
        F.col("c.plan"),
    )
)

with_last_customers_2 = (
    with_last_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.prev_age", F.col("c.shipping_fee"))
)

not_flagged = (
    with_last_customers_2
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (~F.col("x.status").isin(["OPEN", "FROZEN"])), "left_semi")
)

result = (
    not_flagged
    .select("id", "shipping_fee")
    .orderBy(F.col("id").desc_nulls_last())
)
