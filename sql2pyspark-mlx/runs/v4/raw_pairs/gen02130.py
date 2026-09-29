from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-04-26").cast("date")) & F.col("segment").isNotNull())
    .select(
        "id",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.upper("email").alias("email_upper"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.account_type").isin(["CHECKING", "LOAN"])), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.browser"))
)

latest_match_customers = (
    latest_match
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.opened_date").alias("account_opened_date"),
        F.col("c.balance").alias("account_balance"),
    )
)

grouped = (
    latest_match_customers
    .withColumn("age_tier", F.when(F.col("age") >= 62, "HIGH").otherwise("LOW"))
    .withColumn("activity_month", F.date_trunc("month", "account_opened_date"))
    .withColumn("quarter_start", F.date_trunc("quarter", "signup_date"))
    .groupingSets(
        [["age_tier", "activity_month", "quarter_start"], ["age_tier", "activity_month"], []],
        "age_tier",
        "activity_month",
        "quarter_start",
    )
    .agg(
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
)

result = grouped.select("age_tier", "activity_month", "city_list", "segment_list")
