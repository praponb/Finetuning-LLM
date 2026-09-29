from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("age_rounded").desc(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("age_rounded").asc_nulls_first())
w3 = Window.partitionBy("city")

valid = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-09-05").cast("date")) & F.col("city").isNotNull())
    .select("*", F.round("age", 0).alias("age_rounded"))
)

metrics = (
    valid
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.rank().over(w2).alias("age_rounded_rank"),
    )
)

eligible = (
    metrics
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.end_date") < F.col("s.signup_date")), "left_anti")
)

matched_keys = (
    eligible
    .select("segment")
    .intersectAll(
        df_customers
        .filter(F.col("age") < 37)
        .select("segment")
    )
)

kept_rows = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")), "inner")
    .select("s.*")
)

remaining_keys = (
    kept_rows
    .select("id", "city")
    .intersect(
        df_customers
        .filter(F.col("segment").isNotNull())
        .select("id", "city")
    )
)

verified = (
    kept_rows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

metrics_customers = (
    verified
    .filter(F.length("name") < 9)
    .select(
        "country",
        "city",
        "segment",
        "is_active",
        "age",
        "age_rounded",
        "event_seq",
        "age_rounded_rank",
        F.count("age_rounded_rank").over(w3).alias("city_count_age_rounded_rank"),
    )
)

valid_customers = (
    metrics_customers
    .filter(F.col("segment").isNotNull() & (F.col("city").eqNullSafe(F.lit("London"))))
    .select(
        "country",
        "city",
        "segment",
        "is_active",
        "age",
        "age_rounded",
        "event_seq",
        "age_rounded_rank",
        F.col("is_active").cast("int").alias("is_active_flag"),
    )
)

result = (
    valid_customers
    .groupBy(
        "country",
        F.when(F.col("age_rounded_rank") >= 7, "LARGE").otherwise("SMALL").alias("age_rounded_rank_tier"),
    )
    .agg(
        F.bit_and("age").alias("age_bit_and"),
        F.size(F.collect_set("segment")).alias("num_segments"),
        F.size(F.collect_set("city")).alias("num_cities"),
    )
    .orderBy(F.col("num_cities").desc())
)
