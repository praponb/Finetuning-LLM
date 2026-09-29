from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("segment", "city").orderBy("credit_limit", "id").rowsBetween(-2, Window.currentRow)
w_hist_2 = Window.partitionBy("country").orderBy(F.col("age").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc_nulls_first(), "id")

base = (
    df_customers
    .filter(F.col("segment") == "Consumer")
    .select(
        "*",
        F.concat("email", F.lit(", "), "city").alias("email_city"),
        F.coalesce("segment", F.lit("NONE")).alias("resolved_segment"),
    )
)

enriched = (
    base
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "credit_limit",
        F.sum("age").over(w_hist).alias("last3_sum_age"),
        F.min("age").over(w_hist).alias("last3_min_age"),
        F.count("age").over(w_hist).alias("last3_count_age"),
        F.avg("age").over(w_hist).alias("last3_avg_age"),
        F.cume_dist().over(w_hist_2).alias("age_cume_dist"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.city",
        "s.last3_sum_age",
        "s.age_cume_dist",
        F.col("c.opened_date"),
        F.col("c.account_type"),
    )
)

eligible = (
    latest_match
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.started_at") < F.col("s.opened_date")), "left_anti")
)

latest = (
    eligible
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select("id", "account_type", "age_cume_dist", "last3_sum_age")
    .unpivot(
        ids=["id", "account_type"],
        values=["age_cume_dist", "last3_sum_age"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
