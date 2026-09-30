from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")
w2 = Window.partitionBy("country", "city").orderBy("rank_in_group", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("country")
w4 = Window.orderBy(F.col("last3_max_credit_limit").asc_nulls_last())
w_grp = Window.orderBy("country_avg_credit_limit", "id")
w6 = Window.orderBy(F.col("age").desc())

kept = (
    df_customers
    .alias("s")
    .join(df_sessions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.started_at") >= F.col("s.signup_date")), "left_anti")
)

kept_customers = (
    kept
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.started_at") <= F.col("s.signup_date")), "left_semi")
)

latest = (
    kept_customers
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

scored = (
    latest
    .select(
        "id",
        "name",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.max("credit_limit").over(w2).alias("last3_max_credit_limit"),
        F.min("credit_limit").over(w2).alias("last3_min_credit_limit"),
        F.count("credit_limit").over(w2).alias("last3_count_credit_limit"),
        F.avg("credit_limit").over(w3).alias("country_avg_credit_limit"),
    )
)

enriched = (
    scored
    .filter(F.col("credit_limit") < 5100)
    .select(
        "id",
        "is_active",
        "credit_limit",
        "last3_max_credit_limit",
        "last3_min_credit_limit",
        F.rank().over(w4).alias("last3_max_credit_limit_rank"),
        F.lead("age", 2).over(w_grp).alias("next_age"),
        F.cume_dist().over(w6).alias("age_cume_dist"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("next_age") >= 67, "LARGE").otherwise("SMALL").alias("next_age_tier"),
    )
    .agg(
        F.percentile_approx("age_cume_dist", [0.25, 0.5, 0.75]).alias("age_cume_dist_quartiles"),
        F.percentile_approx("last3_max_credit_limit", 0.95).alias("last3_max_credit_limit_p95"),
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy("distinct_ids")
)
