from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("segment").orderBy("age", "id")
w3 = Window.partitionBy("city").orderBy("signup_date", "id")
w4 = Window.partitionBy("segment", "country").orderBy(F.col("age").desc())
w5 = Window.partitionBy("city", "segment").orderBy(F.col("age_cume_dist").desc())
w6 = Window.partitionBy("segment").orderBy("age_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("segment").orderBy(F.col("country_max_age").desc_nulls_last())
w_recent = Window.partitionBy("city", "segment").orderBy("age_cume_dist", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_customers
    .filter((F.col("signup_date") > F.lit("2023-08-26").cast("date")) & F.col("segment").isNotNull())
    .withColumn("city_name_key", F.concat_ws("/", "city", "name"))
    .withColumn("email_normalized", F.upper(F.trim("email")))
    .withColumn("city_filled", F.coalesce("city", F.lit("NONE")))
)

eligible = (
    valid
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.status") == "CANCELLED"), "left_anti")
)

enriched = (
    eligible
    .select(
        "id",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "email_normalized",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.lag("age", 2).over(w2).alias("prev_age"),
        F.max("age").over(w).alias("country_max_age"),
        F.row_number().over(w3).alias("row_seq"),
        F.cume_dist().over(w4).alias("age_cume_dist"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w5).alias("age_cume_dist_percent_rank"),
        F.sum("row_seq").over(w6).alias("cumulative_row_seq"),
        F.rank().over(w7).alias("country_max_age_rank"),
        F.avg("prev_age").over(w_recent).alias("prev_age_avg_2"),
    )
)

result = (
    with_windows
    .withColumn("activity_month", F.date_trunc("MONTH", "signup_date"))
    .groupingSets(
        [["segment", "activity_month"], ["segment"], []],
        "segment",
        "activity_month",
    )
    .agg(
        F.round(F.avg("cumulative_row_seq"), 2).alias("avg_cumulative_row_seq"),
        F.count("*").alias("num_rows"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.avg(F.when(F.col("segment") == "Consumer", F.col("age_cume_dist"))).alias("consumer_avg_age_cume_dist"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("consumer_avg_age_cume_dist").asc_nulls_last())
)
