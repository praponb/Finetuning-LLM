from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("status").orderBy(F.col("age").asc_nulls_last(), "id")
w3 = Window.partitionBy("city")

with_latest = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.discount").alias("order_discount"))
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.end_date") < F.col("s.signup_date")), "left_anti")
)

latest = (
    not_flagged
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

remaining_keys = (
    latest
    .select("city", "segment")
    .subtract(
        df_customers
        .filter(F.col("segment").isNotNull())
        .select("city", "segment")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "name",
        "email",
        "is_active",
        "credit_limit",
        "age",
        "status",
        "order_discount",
        "rank_in_group",
        F.corr("age", "credit_limit").over(w3).alias("corr_age_credit_limit"),
    )
)

result = (
    scored
    .select(
        "name",
        "email",
        "is_active",
        "order_discount",
        "rank_in_group",
        "corr_age_credit_limit",
    )
)
