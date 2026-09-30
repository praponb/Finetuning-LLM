from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id")
w2 = Window.partitionBy("segment").orderBy("city_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-07-11"))) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        F.length("city").alias("city_length"),
        F.initcap(F.trim("city")).alias("city_title"),
    )
)

scored = (
    valid
    .withColumn("next_credit_limit", F.lead("credit_limit", 1).over(w))
    .withColumn("first_known_credit_limit", F.first("credit_limit", ignorenulls=True).over(w2))
)

kept = (
    scored
    .alias("s")
    .join(df_subscriptions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.monthly_fee").between(60, 110)), "left_semi")
)

grouped = (
    kept
    .groupBy("city")
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("next_credit_limit", 0.95).alias("next_credit_limit_p95"),
        F.percentile_approx("city_length", [0.25, 0.5, 0.75]).alias("city_length_quartiles"),
        F.sum(F.when(F.col("segment") == "Consumer", F.col("next_credit_limit")).otherwise(0)).alias("consumer_next_credit_limit"),
        F.count("*").alias("row_count"),
    )
)

result = (
    grouped
    .select(
        "city",
        "distinct_ids",
        "next_credit_limit_p95",
        "city_length_quartiles",
        "consumer_next_credit_limit",
        "row_count",
    )
    .orderBy(F.col("consumer_next_credit_limit").asc_nulls_last(), "distinct_ids")
)
