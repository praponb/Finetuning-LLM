from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc_nulls_first())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

ranked = (
    df_customers
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

with_latest = (
    ranked
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"))
)

base = (
    with_latest
    .filter((F.col("signup_date") > F.lit("2023-06-09").cast("date")) & (F.col("city").eqNullSafe(F.lit("Singapore"))))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "row_num",
        "status",
        F.regexp_replace("email", " ", "_").alias("email_slug"),
        F.initcap(F.trim("status")).alias("status_title"),
        F.coalesce("segment", F.lit("NONE")).alias("segment_clean"),
    )
)

not_flagged = (
    base
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.month(F.col("ch.opened_date")).isin([3, 5, 9])), "left_anti")
)

result = (
    not_flagged
    .groupBy("city")
    .pivot("status", ["SHIPPED", "COMPLETED"])
    .agg(F.avg("credit_limit"))
    .select(
        "city",
        F.col("SHIPPED").alias("shipped"),
        F.col("COMPLETED").alias("completed"),
    )
)
