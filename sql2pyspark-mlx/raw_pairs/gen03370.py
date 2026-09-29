from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.month("signup_date"), "city").orderBy("age", "id")
w3 = Window.partitionBy(F.year("signup_date")).orderBy("signup_date", "id").rowsBetween(-6, -1)

not_flagged = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") >= F.col("s.signup_date")), "left_semi")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") >= F.col("s.signup_date")), "left_anti")
)

scored = (
    eligible
    .withColumn("cumulative_age", F.sum("age").over(w_hist))
    .withColumn("next_credit_limit", F.lead("credit_limit", 2).over(w2))
    .withColumn("age_avg_6_prior", F.avg("age").over(w3))
)

base = (
    scored
    .filter((F.col("signup_date") >= F.lit("2023-05-19").cast("date")) & F.col("age_avg_6_prior").between(46, 49))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "age_avg_6_prior",
        F.upper("segment").alias("segment_upper"),
    )
)

result = (
    base
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "credit_limit",
        "age",
        "age_avg_6_prior",
        "segment_upper",
    )
)
