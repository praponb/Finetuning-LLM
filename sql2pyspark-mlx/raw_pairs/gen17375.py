from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.started_at").desc(), F.col("c.id").desc())

filtered = (
    df_customers
    .filter(F.col("segment").isNotNull() & F.col("is_active"))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.trim("email").alias("clean_email"),
        F.concat("city", F.lit(" "), "name").alias("city_name"),
    )
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "city", "age", "clean_email")
)

kept = (
    latest
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.store_id").isNotNull()), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.customer_id").isin([19, 22])), "left_semi")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.device"))
)

result = (
    latest_match
    .groupBy(
        "city",
        F.when(F.col("age") >= 65, "PREMIUM").otherwise("STANDARD").alias("age_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.percentile_approx("age", 0.9).alias("age_p90"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("distinct_ids") < 1000)
    .orderBy(F.col("city").desc())
)
