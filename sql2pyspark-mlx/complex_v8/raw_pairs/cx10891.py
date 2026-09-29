from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("country").orderBy(F.col("days_since_signup_date_month").desc(), F.col("id").desc())

base = (
    df_listeners
    .filter((F.col("birth_year") < 1994) & (F.col("signup_date") < F.lit("2024-12-21").cast("date")))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "birth_year",
        "interests",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("signup_date")).alias("days_since_signup_date"),
        F.date_trunc("month", F.col("signup_date")).alias("signup_date_month"),
    )
)

enriched = (
    base
    .select(
        "id",
        "username",
        "country",
        "signup_date",
        "birth_year",
        "days_since_signup_date",
        "signup_date_month",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("signup_date_month")).alias("days_since_signup_date_month"),
        F.length(F.trim(F.col("username"))).alias("username_len"),
    )
)

latest_per_key = (
    enriched
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key
    .select(
        "id",
        "days_since_signup_date_month",
        "username_len",
        "days_since_signup_date",
        "birth_year",
    )
    .unpivot(
        ids=["id"],
        values=["days_since_signup_date_month", "username_len", "days_since_signup_date", "birth_year"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
