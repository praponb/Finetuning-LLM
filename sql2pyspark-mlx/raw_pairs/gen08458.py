from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date_month").desc_nulls_first())
w2 = Window.partitionBy("country", "city").orderBy(F.col("signup_date_month").desc_nulls_last())

filtered = (
    df_customers
    .filter(F.col("signup_date") >= F.lit("2023-06-17").cast("date"))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        F.month("signup_date").alias("signup_date_month"),
        F.coalesce("city", F.lit("NONE")).alias("city_clean"),
    )
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") < F.col("s.signup_date")), "left_semi")
)

base = (
    not_flagged
    .filter(F.col("city").isNotNull() & (F.col("signup_date") > F.lit("2023-03-15").cast("date")))
    .select("*", F.trim("city").alias("clean_city"))
)

kept_keys = (
    base
    .select("city", "id")
    .subtract(
        df_customers
        .filter(F.col("is_active"))
        .select("city", "id")
    )
)

kept_rows = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    kept_rows
    .withColumn("signup_date_month_cume_dist", F.cume_dist().over(w))
    .withColumn("signup_date_month_rank", F.rank().over(w2))
)

base_customers = (
    with_windows
    .filter(F.col("signup_date") >= "2024-12-01")
    .select(
        "*",
        F.coalesce("segment", F.lit("N/A")).alias("segment_filled"),
        F.coalesce("segment", F.lit("N/A")).alias("segment_clean"),
    )
)

grouped = (
    base_customers
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_rows"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"),
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
    )
)

result = (
    grouped
    .select(
        "city",
        "consumer_rows",
        "num_cities",
        "corporate_count",
        F.when(F.col("city").rlike("^(Tokyo|New York)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy(F.col("city_zone").desc())
)
