from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country", "segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("running_avg_credit_limit").asc_nulls_first(), "id")
w3 = Window.partitionBy("segment").orderBy(F.col("credit_limit").desc())
w4 = Window.partitionBy("country").orderBy("running_count_credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter(F.col("signup_date") >= F.to_date(F.lit("2023-03-22")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        F.when(F.col("country").isin(["TH", "SG"]), "Priority").otherwise("Other").alias("country_group"),
    )
)

with_windows = (
    base
    .filter(F.col("name").isNotNull())
    .select(
        "id",
        "country",
        "segment",
        "signup_date",
        "credit_limit",
        "country_group",
        F.count("credit_limit").over(w_grp).alias("running_count_credit_limit"),
        F.avg("credit_limit").over(w_grp).alias("running_avg_credit_limit"),
        F.min("credit_limit").over(w_grp).alias("running_min_credit_limit"),
        F.sum("credit_limit").over(w_grp).alias("running_sum_credit_limit"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

ranked = (
    latest
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

metrics = (
    ranked
    .withColumn("running_sum_running_min_credit_limit", F.sum("running_min_credit_limit").over(w4))
    .withColumn("running_avg_running_min_credit_limit", F.avg("running_min_credit_limit").over(w4))
    .withColumn("running_min_running_min_credit_limit", F.min("running_min_credit_limit").over(w4))
    .withColumn("running_count_running_min_credit_limit", F.count("running_min_credit_limit").over(w4))
)

kept_keys = (
    metrics
    .select("segment", "country")
    .subtract(
        df_customers
        .filter(F.col("email").isNotNull())
        .select("segment", "country")
    )
)

verified = (
    metrics
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.segment") == F.col("k.segment")) & (F.col("s.country") == F.col("k.country")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_sessions.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.country") == "TH"), "left_anti")
)

result = (
    kept
    .select(
        "id",
        "country",
        "segment",
        "signup_date",
        "country_group",
        "running_count_credit_limit",
        "running_sum_credit_limit",
        "rank_in_group",
    )
    .orderBy(F.col("running_sum_credit_limit").desc())
)
