from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date")
w2 = Window.partitionBy("segment")
w_hist = Window.partitionBy("city")
w4 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc_nulls_last())
w6 = Window.orderBy("age", "id")

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-03-17").cast("date")) & F.col("city").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.substring("email", 1, 3).alias("email_prefix"),
    )
)

deduped = (
    base
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    deduped
    .withColumn("segment_avg_credit_limit", F.avg("credit_limit").over(w2))
    .withColumn("city_min_credit_limit", F.min("credit_limit").over(w_hist))
    .withColumn("first_known_segment", F.first("segment", ignorenulls=True).over(w4))
)

with_windows = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w5).alias("credit_limit_percent_rank"),
        F.ntile(4).over(w6).alias("age_bucket"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "name",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "email_prefix",
        "segment_avg_credit_limit",
        "city_min_credit_limit",
        F.when(F.col("name").rlike("^(  Carol Diaz|Mia Rossi)$"), "CORE").otherwise("EXTENDED").alias("name_zone"),
    )
    .orderBy(F.col("city_min_credit_limit").desc_nulls_last())
)
