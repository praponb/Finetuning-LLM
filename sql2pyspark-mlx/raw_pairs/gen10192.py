from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w_grp = Window.partitionBy("segment").orderBy("credit_limit", "id")

cleaned = (
    df_customers
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.length("name").alias("name_length"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.opened_date").alias("account_opened_date"))
)

kept = (
    latest_match
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") > "2024-07-01"), "left_semi")
)

with_windows = (
    kept
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "age",
        "name_length",
        "city_clean",
        "account_opened_date",
        F.lead("name_length").over(w_grp).alias("next_name_length"),
        F.lag("age").over(w_grp).alias("prev_age"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

daily = (
    with_windows
    .groupBy(
        F.date_trunc("WEEK", "account_opened_date").alias("week_start"),
        "country",
        F.date_trunc("YEAR", "account_opened_date").alias("year_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.count_if(F.col("city") == "Singapore").alias("singapore_events"),
        F.sum(F.when(F.col("city") == "London", F.col("age")).otherwise(0)).alias("london_age"),
    )
)

result = (
    daily
    .select(
        "week_start",
        "country",
        "year_start",
        "num_rows",
        "singapore_events",
        F.when(F.col("country").rlike("^(JP|US)$"), "OTHER_ZONE").when(F.col("country").rlike("^(TH|SG)$"), "GROUP_A").otherwise("CORE").alias("country_zone"),
    )
)
