from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("city").orderBy("credit_limit", "id")

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-02-13").cast("date")) & (F.length("name") >= 14))
    .select(
        "*",
        F.length("country").alias("country_length"),
        F.trim("email").alias("clean_email"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") >= F.col("s.signup_date")), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.email",
        "s.city",
        "s.signup_date",
        "s.is_active",
        "s.credit_limit",
        "s.clean_email",
        "s.city_clean",
        F.col("c.balance"),
    )
)

not_flagged_customers = (
    latest_match
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") <= F.col("s.signup_date")), "left_anti")
)

scored = (
    not_flagged_customers
    .filter(F.col("signup_date") < "2023-09-01")
    .select(
        "*",
        F.lag("credit_limit").over(w_part).alias("prev_credit_limit"),
        F.lead("balance").over(w_part).alias("next_balance"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

valid = (
    scored
    .filter(F.col("next_balance").between(29000, 60000))
    .withColumn("city_email", F.concat("city", F.lit("/"), "email"))
)

result = (
    valid
    .select(
        "id",
        "name",
        "email",
        "city",
        "signup_date",
        "credit_limit",
        "clean_email",
        "city_clean",
        "balance",
    )
    .orderBy(F.col("id").asc_nulls_last())
)
