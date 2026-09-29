from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date_plus_54d").desc_nulls_first(), "id")
w2 = Window.partitionBy("customer_id", "segment").orderBy(F.col("credit_limit").desc_nulls_last(), "id")
w3 = Window.partitionBy("customer_id", "status").orderBy("opened_date_plus_54d", "id").rowsBetween(-3, -1)
w4 = Window.orderBy("credit_limit", "id")

filtered = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2024-02-20").cast("date"))
    .select(
        "id",
        "customer_id",
        "opened_date",
        "status",
        F.date_add("opened_date", 54).alias("opened_date_plus_54d"),
    )
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.name"), F.col("d.credit_limit"), F.col("d.segment"))
)

deduped_accounts = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

with_windows = (
    deduped_accounts
    .withColumn("credit_limit_avg_3_prior", F.avg("credit_limit").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

joined = (
    with_windows
    .alias("e")
    .join(df_customers.alias("b"), F.col("e.customer_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.country"), F.col("b.city"))
)

result = (
    joined
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_count"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.count(F.when(F.col("city") == "Chiang Mai", 1)).alias("chiang_mai_rows"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.percentile_approx("credit_limit", 0.9).alias("credit_limit_p90"),
        F.percentile_approx("credit_limit_avg_3_prior", 0.95).alias("credit_limit_avg_3_prior_p95"),
    )
    .filter(F.col("distinct_segments") < 1)
    .orderBy(F.col("credit_limit_p90").desc_nulls_first())
)
