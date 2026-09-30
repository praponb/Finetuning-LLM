from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("country").orderBy("is_corporate", "id")
w2 = Window.partitionBy(F.year("signup_date")).orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")

base = (
    df_customers
    .filter(F.col("age") < 40)
    .select(
        "*",
        F.concat("email", F.lit(" "), "country").alias("email_country"),
        F.when(F.col("segment") == "Corporate", 1).otherwise(0).alias("is_corporate"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("resolved_segment"),
    )
)

scored = (
    base
    .filter(F.col("age") < 49)
    .select(
        "id",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "resolved_segment",
        F.row_number().over(w_part).alias("row_seq"),
        F.sum("is_corporate").over(w2).alias("running_is_corporate"),
        F.last("credit_limit", ignorenulls=True).over(w3).alias("last_known_credit_limit"),
    )
)

deduped = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "email",
        "country",
        "signup_date",
        "credit_limit",
        "row_seq",
        "running_is_corporate",
        "last_known_credit_limit",
    )
)

kept = (
    deduped
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.year(F.col("c.start_date")) >= 2024), "left_anti")
)

summary = (
    kept
    .withColumn("year_start", F.date_trunc("YEAR", "signup_date"))
    .rollup("year_start")
    .agg(
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.sum("last_known_credit_limit").alias("total_last_known_credit_limit"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("sg_events") < 1000)
)

result = summary.select("sg_events", "total_last_known_credit_limit", "grouping_level")
