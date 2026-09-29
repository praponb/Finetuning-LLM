from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("credit_limit", "id")
w2 = Window.partitionBy("country").orderBy(F.col("signup_date").asc_nulls_first(), "id")
w3 = Window.partitionBy(F.month("signup_date"), "segment").orderBy("signup_date", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("city", "country")
w_ordered = Window.partitionBy("country", "city").orderBy("row_num", "id").rowsBetween(-3, -1)
w6 = Window.partitionBy(F.year("signup_date"), "country")
w7 = Window.partitionBy("country").orderBy(F.col("credit_limit").desc(), "id")
w_grp = Window.orderBy(F.col("age").desc(), "id")
w_hist = Window.partitionBy("segment", "city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("signup_date", "id")
w11 = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.orderBy("next_signup_date_year_count_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy("credit_limit", "id")
w14 = Window.orderBy("next_signup_date_year_count_credit_limit", "id")
w15 = Window.partitionBy(F.date_trunc("month", "signup_date"))

deduped = (
    df_customers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

latest = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    latest
    .filter(F.col("country").isin(["DE", "JP"]))
    .withColumn("age_avg_2_prior", F.avg("age").over(w3))
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w4)), 4))
    .withColumn("age_avg_3_prior", F.avg("age").over(w_ordered))
    .withColumn("signup_date_year_count_credit_limit", F.count("credit_limit").over(w6))
)

deduped_customers = (
    metrics
    .withColumn("row_num_2", F.row_number().over(w7))
    .filter(F.col("row_num_2") == 1)
)

enriched = (
    deduped_customers
    .select(
        "id",
        "name",
        "signup_date",
        "is_active",
        "credit_limit",
        "row_num",
        "age_avg_2_prior",
        "age_avg_3_prior",
        F.ntile(5).over(w_grp).alias("age_bucket"),
        F.sum("age_avg_2_prior").over(w_hist).alias("cumulative_age_avg_2_prior"),
        F.lead("signup_date_year_count_credit_limit", 2).over(w10).alias("next_signup_date_year_count_credit_limit"),
    )
)

scored = (
    enriched
    .withColumn("last_known_next_signup_date_year_count_credit_limit", F.last("next_signup_date_year_count_credit_limit", ignorenulls=True).over(w11))
    .withColumn("cumulative_age_avg_2_prior_2", F.sum("age_avg_2_prior").over(w12))
    .withColumn("credit_limit_bucket", F.ntile(3).over(w13))
    .withColumn("event_seq", F.row_number().over(w14))
    .withColumn("signup_date_period_avg_credit_limit", F.avg("credit_limit").over(w15))
)

result = (
    scored
    .select(
        "id",
        "name",
        "signup_date",
        "is_active",
        "credit_limit",
        "age_bucket",
        "cumulative_age_avg_2_prior",
        "next_signup_date_year_count_credit_limit",
        "cumulative_age_avg_2_prior_2",
    )
    .orderBy(F.col("cumulative_age_avg_2_prior").desc_nulls_first())
)
