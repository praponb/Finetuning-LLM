from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("credit_limit").asc_nulls_last())
w2 = Window.partitionBy("city").orderBy("credit_limit", "id")
w3 = Window.orderBy("credit_limit", "id")
w4 = Window.orderBy("credit_limit_plus_age", "id")
w5 = Window.partitionBy("city")
w6 = Window.partitionBy("country").orderBy("credit_limit_plus_age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy("signup_date")
w8 = Window.partitionBy("country").orderBy(F.col("rank_in_group").desc())

cleaned = (
    df_customers
    .filter(F.length("name") > 6)
    .select(
        "*",
        F.regexp_replace("name", " ", "-").alias("name_slug"),
        (F.col("credit_limit") + F.col("age")).alias("credit_limit_plus_age"),
    )
)

enriched = (
    cleaned
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w))
    .withColumn("credit_limit_bucket", F.ntile(4).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
    .withColumn("next_credit_limit_plus_age", F.lead("credit_limit_plus_age", 1).over(w4))
)

with_windows = (
    enriched
    .withColumn("row_seq_share", F.round((F.col("row_seq") / F.sum("row_seq").over(w5)), 4))
    .withColumn("last_known_credit_limit_cume_dist", F.last("credit_limit_cume_dist", ignorenulls=True).over(w6))
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.rank().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

valid = (
    latest
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "name_slug",
        "credit_limit_plus_age",
        "credit_limit_cume_dist",
        "credit_limit_bucket",
        "row_seq",
        "next_credit_limit_plus_age",
        "rank_in_group",
        F.when(F.col("credit_limit_plus_age") >= 85, "High").when(F.col("credit_limit_plus_age") >= 14, "Medium").otherwise("Low").alias("credit_limit_plus_age_level"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.rank().over(w8))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

remaining_keys = (
    deduped
    .select("id", "country")
    .intersect(
        df_customers
        .filter(F.length("email") > 12)
        .select("id", "country")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

daily = (
    kept_rows
    .groupBy(
        F.date_trunc("WEEK", "signup_date").alias("week_start"),
        F.date_trunc("QUARTER", "signup_date").alias("quarter_start"),
        F.date_trunc("MONTH", "signup_date").alias("activity_month"),
    )
    .agg(
        F.bit_or("age").alias("age_bit_or"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("country") == "US", 1)), F.lit(0)), 2).alias("credit_limit_per_us"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.percentile_approx("age", 0.95).alias("age_p95"),
    )
    .filter(F.col("credit_limit_per_us") >= 0)
)

result = (
    daily
    .select(
        "week_start",
        "quarter_start",
        "activity_month",
        "age_bit_or",
        "num_segments",
        "city_list",
        "age_p95",
    )
)
