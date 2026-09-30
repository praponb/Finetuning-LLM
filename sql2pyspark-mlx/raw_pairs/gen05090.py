from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "signup_date"), "segment").orderBy("signup_date", "id")
w2 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("age", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("credit_limit", "id")
w5 = Window.partitionBy("segment").orderBy(F.col("signup_date").desc(), "id")
w6 = Window.partitionBy("country").orderBy("last_known_credit_limit", "id")
w7 = Window.partitionBy("city", "segment").orderBy(F.col("event_seq_2").desc(), "id")

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-09-03").cast("date")) & (F.col("city") == "Singapore"))
    .withColumn("clean_email", F.trim("email"))
    .withColumn("resolved_city", F.coalesce("city", F.lit("N/A")))
)

enriched = (
    filtered
    .withColumn("next_age", F.lead("age", 1).over(w))
    .withColumn("last_known_credit_limit", F.last("credit_limit", ignorenulls=True).over(w2))
    .withColumn("credit_limit_avg_2", F.avg("credit_limit").over(w3))
)

scored = (
    enriched
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "resolved_city",
        "last_known_credit_limit",
        "credit_limit_avg_2",
        F.lead("credit_limit_avg_2", 1).over(w4).alias("next_credit_limit_avg_2"),
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w6).alias("event_seq_2"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .select(
        "id",
        "country",
        "city",
        "signup_date",
        "is_active",
        "credit_limit",
        "resolved_city",
        "next_credit_limit_avg_2",
        "event_seq",
        F.when(F.col("city").rlike("^(New York|London)$"), "EXTENDED").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "CORE").otherwise("OTHER_ZONE").alias("city_zone"),
    )
    .orderBy(F.col("event_seq").asc_nulls_first(), F.col("country").desc())
)
