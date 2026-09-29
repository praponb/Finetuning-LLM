from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").asc_nulls_first())
w2 = Window.partitionBy("segment").orderBy(F.col("balance").desc_nulls_last())

enriched = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id").eqNullSafe(F.col("p.id")), "inner")
    .filter(F.col("p.name").like("Mia%"))
    .select(
        "s.id",
        "s.customer_id",
        "s.balance",
        "s.opened_date",
        F.col("p.segment"),
        F.col("p.country"),
    )
)

matched_keys = (
    enriched
    .select("id", "customer_id")
    .intersectAll(
        df_accounts
        .filter(F.col("balance") > 73000)
        .select("id", "customer_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.percent_rank().over(w).alias("balance_percent_rank"),
        F.cume_dist().over(w2).alias("balance_cume_dist"),
    )
)

filtered = (
    metrics
    .filter((F.col("country").eqNullSafe(F.lit("US"))) & F.col("country").isin(["JP", "TH", "US"]))
    .select(
        "id",
        "balance",
        "opened_date",
        "country",
        "balance_percent_rank",
        "balance_cume_dist",
        F.date_trunc("YEAR", "opened_date").alias("opened_date_period"),
    )
)

remaining_keys = (
    filtered
    .select("id")
    .subtract(
        df_accounts
        .filter(F.col("account_type").isin(["LOAN", "CREDIT", "CHECKING"]))
        .select("id")
    )
)

reconciled_accounts = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled_accounts
    .withColumn("balance_cume_dist_tier", F.when(F.col("balance_cume_dist") >= 0.87, "PREMIUM").otherwise("STANDARD"))
    .withColumn("balance_tier", F.when(F.col("balance") >= 83000, "PREMIUM").otherwise("STANDARD"))
    .cube("balance_cume_dist_tier", "balance_tier")
    .agg(
        F.sum("balance").alias("total_balance"),
        F.sum(F.when(F.col("country") == "SG", F.col("balance")).otherwise(0)).alias("sg_balance"),
        F.avg(F.when(F.col("country") == "US", F.col("balance_percent_rank"))).alias("us_avg_balance_percent_rank"),
        F.grouping("balance_cume_dist_tier").alias("gid"),
    )
)
