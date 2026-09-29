from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("is_active_flag").desc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("country").orderBy(F.col("signup_date").desc())

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-05-20").cast("date")) & F.col("segment").isNotNull())
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("NONE")))
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "name",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "is_active_flag",
        "resolved_city",
    )
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") >= F.col("s.signup_date")), "left_semi")
)

not_flagged_customers = (
    not_flagged
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.order_date") >= F.col("s.signup_date")), "left_semi")
)

not_flagged_customers_2 = (
    not_flagged_customers
    .alias("s")
    .join(df_accounts.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.opened_date") >= F.col("s.signup_date")), "left_semi")
)

with_latest = (
    not_flagged_customers_2
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status"), F.col("c.shipping_fee"))
)

ranked = (
    with_latest
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
)

result = (
    ranked
    .rollup("status")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("is_active_flag"))).alias("cancelled_total_is_active_flag"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
        F.percentile_approx("shipping_fee", 0.5).alias("shipping_fee_p50"),
        F.grouping_id().alias("grouping_level"),
    )
)
