from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment")
w2 = Window.orderBy(F.col("age").desc_nulls_first())
w_grp = Window.partitionBy("segment", "country").orderBy(F.col("age_cume_dist").asc_nulls_first())
w_running = Window.orderBy(F.col("age").desc_nulls_last())

kept_keys = (
    df_customers
    .select("country", "id")
    .exceptAll(
        df_customers
        .filter(~F.col("name").like("H%"))
        .select("country", "id")
    )
)

kept_rows = (
    df_customers
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.length("city").alias("city_length"),
    )
)

metrics = (
    filtered
    .withColumn("segment_avg_city_length", F.avg("city_length").over(w))
    .withColumn("age_cume_dist", F.cume_dist().over(w2))
)

cleaned = (
    metrics
    .filter(F.col("country") == "US")
    .withColumn("segment_lower", F.lower("segment"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("NONE")))
)

filtered_customers = (
    cleaned
    .filter(F.col("segment").isNotNull())
    .withColumn("signup_date_plus_18d", F.date_add("signup_date", 18))
    .withColumn("signup_date_plus_45d", F.date_add("signup_date", 45))
)

result = (
    filtered_customers
    .select(
        "*",
        F.cume_dist().over(w_grp).alias("age_cume_dist_cume_dist"),
        F.dense_rank().over(w_running).alias("age_dense_rank"),
    )
)
