from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("signup_date").asc_nulls_last(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())

valid = (
    df_customers
    .filter(F.col("signup_date") > F.to_date(F.lit("2023-05-17")))
    .withColumn("segment_upper", F.upper("segment"))
    .withColumn("name_country_key", F.concat_ws("-", "name", "country"))
)

deduped = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "segment_upper",
        "name_country_key",
    )
)

with_latest = (
    deduped
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.segment",
        "s.segment_upper",
        F.col("c.amount").alias("order_amount"),
    )
)

kept = (
    with_latest
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.status") == "EXPIRED"), "left_semi")
)

remaining_keys = (
    kept
    .select("id")
    .subtract(
        df_customers
        .filter(F.col("email").like("%@GMAIL.COM"))
        .select("id")
    )
)

verified = (
    kept
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("segment")
    .agg(
        F.round(F.sum("order_amount") / F.nullif(F.count(F.when(F.col("segment") == "SMB", 1)), F.lit(0)), 2).alias("order_amount_per_smb"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
)
