from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("country").orderBy("signup_date", "id")
w2 = Window.partitionBy("segment")

cleaned = (
    df_customers
    .filter((F.col("signup_date") > F.to_date(F.lit("2023-08-02"))) & F.col("segment").isNotNull())
    .select("*", F.col("is_active").cast("int").alias("is_active_flag"))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date") >= F.col("s.signup_date")), "left_anti")
)

base = (
    not_flagged
    .filter((F.col("segment").eqNullSafe(F.lit("SMB"))) & (F.col("signup_date") >= "2023-09-01"))
    .withColumn("email_country", F.concat("email", F.lit(" - "), "country"))
    .withColumn("name_filled", F.coalesce("name", F.lit("unassigned")))
    .withColumn("resolved_city", F.coalesce("city", F.lit("OTHER")))
)

enriched = (
    base
    .select(
        "*",
        F.lag("credit_limit").over(w_recent).alias("prev_credit_limit"),
        F.lead("is_active_flag").over(w_recent).alias("next_is_active_flag"),
        F.row_number().over(w_recent).alias("event_seq"),
        F.corr("credit_limit", "age").over(w2).alias("corr_credit_limit_age"),
    )
)

result = (
    enriched
    .groupBy(F.date_trunc("YEAR", "signup_date").alias("year_start"))
    .agg(
        F.count_if(F.col("segment") == "SMB").alias("smb_events"),
        F.sum(F.when(F.col("city") == "New York", F.col("age")).otherwise(0)).alias("new_york_age"),
        F.sum("event_seq").alias("total_event_seq"),
        F.corr("age", "credit_limit").alias("corr_age_credit_limit"),
        F.min("next_is_active_flag").alias("min_next_is_active_flag"),
        F.round(F.avg("corr_credit_limit_age"), 2).alias("avg_corr_credit_limit_age"),
    )
)
