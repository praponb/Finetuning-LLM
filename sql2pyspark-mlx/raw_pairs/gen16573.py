from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("segment").orderBy(F.col("age").desc_nulls_first())
w4 = Window.partitionBy("city").orderBy(F.col("signup_date").desc(), "id")
w5 = Window.partitionBy("city").orderBy("credit_limit", "id")
w6 = Window.partitionBy(F.year("signup_date"), "country").orderBy(F.col("credit_limit_bucket").asc_nulls_last())
w7 = Window.partitionBy("country")
w8 = Window.partitionBy("country").orderBy("credit_limit_share", "id")
w_part = Window.partitionBy("city", "country").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("credit_limit_bucket", "id")
w_hist = Window.partitionBy("country").orderBy("credit_limit_bucket_share", "id")

valid = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-05-01").cast("date")) & ~F.col("name").like("FR%"))
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.concat("name", F.lit(" - "), "email").alias("name_email"),
        F.concat("segment", F.lit(", "), "email").alias("segment_email"),
    )
)

enriched = (
    valid
    .filter(F.col("age").between(35, 64))
    .select(
        "id",
        "name",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "segment_email",
        F.sum("age").over(w).alias("running_age"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4).alias("credit_limit_share"),
        F.rank().over(w3).alias("age_rank"),
    )
)

enriched_customers = (
    enriched
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("credit_limit_bucket", F.ntile(3).over(w5))
)

enriched_customers_2 = (
    enriched_customers
    .select(
        "*",
        F.percent_rank().over(w6).alias("credit_limit_bucket_percent_rank"),
        F.round((F.col("credit_limit_bucket") / F.sum("credit_limit_bucket").over(w7)), 4).alias("credit_limit_bucket_share"),
        F.lag("credit_limit_share", 1).over(w8).alias("prev_credit_limit_share"),
    )
)

metrics = (
    enriched_customers_2
    .withColumn("running_count_event_seq", F.count("event_seq").over(w_part))
    .withColumn("running_sum_event_seq", F.sum("event_seq").over(w_part))
    .withColumn("running_max_event_seq", F.max("event_seq").over(w_part))
    .withColumn("next_credit_limit_share", F.lead("credit_limit_share", 2).over(w8))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_subscriptions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.end_date") < F.col("s.signup_date")), "left_anti")
)

scored = (
    not_flagged
    .select(
        "*",
        F.row_number().over(w10).alias("event_seq_2"),
        F.lead("running_sum_event_seq", 1).over(w_hist).alias("next_running_sum_event_seq"),
    )
)

result = (
    scored
    .select(
        "id",
        "name",
        "country",
        "city",
        "signup_date",
        "segment_email",
        "running_age",
        "credit_limit_share",
        "age_rank",
        F.when(F.col("city").rlike("^(Berlin|London)$"), "EXTENDED").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "GROUP_B").otherwise("GROUP_A").alias("city_zone"),
    )
)
