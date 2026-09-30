from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter((~F.col("segment").eqNullSafe(F.lit("SMB"))) & (F.col("segment") == "Corporate"))
    .select(
        "id",
        "email",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.date_format("signup_date", "MMM yyyy").alias("signup_date_label"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
    )
)

filtered = (
    valid
    .filter(F.col("signup_date") >= F.lit("2023-09-07").cast("date"))
    .select(
        "id",
        "email",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "segment_filled",
        F.coalesce("email", F.lit("Unknown")).alias("email_filled"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") < F.col("s.signup_date")), "left_anti")
)

with_last = (
    kept
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("order_amount"))
)

with_last_customers = (
    with_last
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status").alias("order_status"),
        F.col("c.order_date").alias("order_order_date"),
    )
)

result = (
    with_last_customers
    .select("id", "email", "credit_limit", "age", "segment_filled", "email_filled")
)
