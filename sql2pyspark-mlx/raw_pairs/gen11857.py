from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("balance")
w2 = Window.partitionBy(F.date_trunc("MONTH", "signup_date"), "country").orderBy(F.col("balance").desc_nulls_first())
w3 = Window.orderBy("balance")
w4 = Window.partitionBy("country", "status").orderBy(F.col("balance").asc_nulls_last())
w5 = Window.partitionBy("account_type")
w6 = Window.orderBy(F.col("balance_dense_rank_2").desc_nulls_last())
w7 = Window.partitionBy("country").orderBy(F.col("balance_dense_rank").desc_nulls_last())
w8 = Window.partitionBy(F.date_trunc("MONTH", "signup_date"), "country").orderBy(F.col("balance").asc_nulls_first())
w9 = Window.partitionBy("customer_id")

valid = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-05-22").cast("date"))
    .select(
        "*",
        F.col("balance").cast("int").alias("balance_int"),
        F.date_trunc("WEEK", "opened_date").alias("opened_date_period"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_filled"),
    )
)

enriched = (
    valid
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id").eqNullSafe(F.col("p.id")), "left")
    .select(
        "src.id",
        "src.customer_id",
        "src.account_type",
        "src.balance",
        "src.status",
        F.col("p.signup_date"),
        F.col("p.country"),
    )
)

remaining_keys = (
    enriched
    .select("id")
    .subtract(
        df_accounts
        .filter(F.col("customer_id").isNull())
        .select("id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    reconciled
    .select(
        "*",
        F.cume_dist().over(w).alias("balance_cume_dist"),
        F.dense_rank().over(w2).alias("balance_dense_rank"),
        F.rank().over(w3).alias("balance_rank"),
        F.dense_rank().over(w4).alias("balance_dense_rank_2"),
    )
)

matched_keys = (
    scored
    .select("status")
    .subtract(
        df_accounts
        .filter(F.col("balance") > 33000)
        .select("status")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

scored_accounts = (
    verified
    .select(
        "*",
        F.min("balance_rank").over(w5).alias("account_type_min_balance_rank"),
        F.cume_dist().over(w6).alias("balance_dense_rank_2_cume_dist"),
        F.rank().over(w7).alias("balance_dense_rank_rank"),
        F.rank().over(w8).alias("balance_rank_2"),
        F.count("balance_dense_rank_2").over(w9).alias("customer_id_count_balance_dense_rank_2"),
    )
)

grouped = (
    scored_accounts
    .groupBy(
        F.when(F.col("balance_dense_rank_2_cume_dist") >= 0.8, "LARGE").otherwise("SMALL").alias("balance_dense_rank_2_cume_dist_tier"),
    )
    .agg(
        F.round(F.sum("balance_dense_rank") / F.nullif(F.count(F.when(F.col("country") == "TH", 1)), F.lit(0)), 2).alias("balance_dense_rank_per_th"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
    )
)

result = (
    grouped
    .select(
        "balance_dense_rank_2_cume_dist_tier",
        "balance_dense_rank_per_th",
        "closed_count",
    )
)
