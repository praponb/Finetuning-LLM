from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").asc_nulls_last(), "id")
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("city").orderBy("credit_limit", "id")
w4 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("country", "city").orderBy("latest_rank", "id").rowsBetween(-6, Window.currentRow)
w_recent = Window.partitionBy("segment", "country").orderBy("latest_rank", "id")
w7 = Window.partitionBy("city", "segment")
w8 = Window.orderBy(F.col("country_count_credit_limit").desc(), "id")
w9 = Window.partitionBy("segment").orderBy(F.col("row_seq").desc(), "id")
w10 = Window.partitionBy("country").orderBy("row_seq", "id")
w11 = Window.partitionBy("country", "city")
w12 = Window.orderBy("prev_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-10-31").cast("date")) & F.col("segment").isNotNull() & F.col("is_active"))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        F.lower("name").alias("name_lower"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    deduped
    .withColumn("country_count_credit_limit", F.count("credit_limit").over(w2))
    .withColumn("prev_credit_limit", F.lag("credit_limit", 2).over(w3))
    .withColumn("running_credit_limit", F.sum("credit_limit").over(w4))
    .withColumn("credit_limit_avg_6", F.avg("credit_limit").over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_recent))
)

kept = (
    with_windows
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") >= F.col("s.signup_date")), "left_anti")
)

scored = (
    kept
    .select(
        "*",
        F.avg("country_count_credit_limit").over(w7).alias("city_avg_country_count_credit_limit"),
        F.row_number().over(w8).alias("row_seq_2"),
        F.ntile(5).over(w9).alias("row_seq_bucket"),
        F.ntile(10).over(w10).alias("row_seq_bucket_2"),
    )
)

filtered = (
    scored
    .filter((F.col("signup_date") > F.lit("2023-12-24").cast("date")) & F.col("segment").isNotNull())
    .select(
        "*",
        F.substring("city", 1, 3).alias("city_prefix"),
        F.ceil("latest_rank").alias("latest_rank_whole"),
        F.coalesce("segment", F.lit("DIRECT")).alias("resolved_segment"),
    )
)

scored_customers = (
    filtered
    .withColumn("country_count_credit_limit_share", F.round((F.col("country_count_credit_limit") / F.sum("country_count_credit_limit").over(w11)), 4))
    .withColumn("first_known_row_seq_2", F.first("row_seq_2", ignorenulls=True).over(w12))
)

result = (
    scored_customers
    .select(
        "id",
        "segment",
        F.col("country_count_credit_limit_share").alias("COUNTRY_COUNT_CREDIT_LIMIT_SHARE"),
        F.col("credit_limit").alias("CREDIT_LIMIT"),
    )
    .unpivot(
        ["id", "segment"],
        ["COUNTRY_COUNT_CREDIT_LIMIT_SHARE", "CREDIT_LIMIT"],
        "measure",
        "val",
    )
)
