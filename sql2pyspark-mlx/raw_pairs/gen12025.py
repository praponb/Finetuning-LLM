from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("quarter_start").orderBy(F.col("distinct_ids").desc())

base = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-09-28"))) & (F.col("status") == "OPEN"))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        F.initcap(F.trim("status")).alias("status_title"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

result = (
    base
    .groupBy(
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
        "customer_id",
    )
    .agg(
        F.percentile_approx("balance", 0.95).alias("balance_p95"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles_2"),
        F.max("balance").alias("max_balance"),
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("balance").alias("balance_stddev"),
    )
    .filter(F.col("balance_p95") < 1000000)
    .withColumn("distinct_ids_rank", F.rank().over(w))
    .orderBy(F.col("balance_stddev").desc_nulls_first())
)
