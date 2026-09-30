from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("country").orderBy("credit_limit", "id")

metrics = (
    df_customers
    .select(
        "*",
        F.lead("age").over(w_running).alias("next_age"),
        F.lag("age").over(w_running).alias("prev_age"),
        F.lead("credit_limit").over(w_running).alias("next_credit_limit"),
        F.lag("credit_limit").over(w_running).alias("prev_credit_limit"),
        F.row_number().over(w_running).alias("event_seq"),
    )
)

result = (
    metrics
    .cube(
        F.date_trunc("quarter", "signup_date").alias("quarter_start"),
        F.date_trunc("month", "signup_date").alias("activity_month"),
    )
    .agg(
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
        F.percentile_approx("event_seq", 0.95).alias("event_seq_p95"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.corr("credit_limit", "age").alias("corr_credit_limit_age"),
        F.corr("credit_limit", "age").alias("corr_credit_limit_age_2"),
    )
    .filter(F.col("corr_credit_limit_age") >= 0)
)
