from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment")
w2 = Window.partitionBy("country")
w3 = Window.partitionBy(F.year("signup_date"))
w4 = Window.orderBy("corr_age_credit_limit", "id")
w5 = Window.partitionBy("city").orderBy("age_share", "id")
w6 = Window.partitionBy("city").orderBy("age_share")
w7 = Window.partitionBy("segment").orderBy(F.col("age_share_rank").asc_nulls_first(), "id")

kept = (
    df_customers
    .alias("s")
    .join(df_accounts.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.opened_date") >= F.col("s.signup_date")), "left_anti")
)

metrics = (
    kept
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "is_active",
        "age",
        F.corr("age", "credit_limit").over(w).alias("corr_age_credit_limit"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
        F.corr("credit_limit", "age").over(w3).alias("corr_credit_limit_age"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.row_number().over(w4).alias("seq_no"),
        F.row_number().over(w5).alias("event_seq"),
        F.rank().over(w6).alias("age_share_rank"),
    )
)

ranked = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "name",
        "city",
        "segment",
        "corr_age_credit_limit",
        "age_share",
        "corr_credit_limit_age",
        "event_seq",
    )
)

kept_keys = (
    ranked
    .select("city")
    .subtract(
        df_customers
        .filter(F.col("age") < 47)
        .select("city")
    )
)

reconciled = (
    ranked
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

eligible = (
    reconciled
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") >= "2025-04-01"), "left_semi")
)

wide = (
    eligible
    .groupBy("city")
    .pivot("segment", ["Consumer", "Corporate"])
    .agg(F.max("event_seq").alias("value_sum"))
    .select(
        "city",
        F.col("Consumer").alias("consumer"),
        F.col("Corporate").alias("corporate"),
    )
)

result = (
    wide
    .select(
        "city",
        F.when(F.col("city").rlike("^(Singapore|Berlin)$"), "GROUP_A").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("city_zone"),
    )
)
