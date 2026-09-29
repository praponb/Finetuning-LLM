from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.end_date").desc(), F.col("c.id").desc())

latest_match = (
    df_customers
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") <= F.col("s.signup_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.city",
        "s.is_active",
        "s.credit_limit",
        "s.age",
        F.col("c.end_date"),
        F.col("c.monthly_fee"),
    )
)

result = (
    latest_match
    .select(
        "id",
        "name",
        "email",
        "city",
        "is_active",
        "credit_limit",
        "age",
        "end_date",
        "monthly_fee",
    )
)
