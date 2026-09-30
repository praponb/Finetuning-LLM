from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit")
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("city", "country")
w4 = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(-2, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-03-08").cast("date")) & F.col("segment").isNotNull())
    .select("*", F.trim("country").alias("clean_country"))
)

eligible = (
    base
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") >= F.col("s.signup_date")), "left_semi")
)

deduped = (
    eligible
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "signup_date",
        "is_active",
        "age",
        "rank_in_group",
        F.corr("age", "credit_limit").over(w2).alias("corr_age_credit_limit"),
        F.max("credit_limit").over(w3).alias("city_max_credit_limit"),
        F.avg("credit_limit").over(w4).alias("credit_limit_avg_2"),
    )
)

with_last = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.country",
        "s.signup_date",
        "s.is_active",
        "s.age",
        "s.credit_limit_avg_2",
        F.col("c.balance"),
        F.col("c.opened_date").alias("account_opened_date"),
    )
)

result = (
    with_last
    .withColumn("age_tier", F.when(F.col("age") >= 57, "PREMIUM").otherwise("STANDARD"))
    .cube("age_tier", "country")
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.sum(F.when(F.col("country") == "JP", F.col("age"))).alias("jp_total_age"),
        F.avg(F.when(F.col("country") == "UK", F.col("age"))).alias("uk_avg_age"),
        F.grouping("age_tier").alias("grouping_level"),
    )
    .orderBy(F.col("age_tier").desc())
)
