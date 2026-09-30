from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy(F.col("balance").desc_nulls_first())
w_recent = Window.partitionBy("account_type", "status").orderBy("opened_date", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")

base = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-05-29").cast("date"))
    .select(
        "id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.when(F.col("status").isin(["CLOSED", "FROZEN"]), "Group A").otherwise("Other").alias("status_group"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id") == 32), "left_semi")
)

filtered = (
    eligible
    .filter((F.col("opened_date") > F.lit("2023-10-01").cast("date")) & F.col("status").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "account_type", "status").alias("account_type_status_key"),
        F.coalesce("status", F.lit("N/A")).alias("status_filled"),
    )
)

metrics = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w_grp).alias("balance_dense_rank"),
        F.avg("balance").over(w_recent).alias("balance_avg_6_prior"),
    )
)

latest = (
    metrics
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

base_accounts = (
    latest
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-09-01")))
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "account_type_status_key",
        "status_filled",
        "balance_avg_6_prior",
        "row_num",
        F.floor("balance_avg_6_prior").alias("balance_avg_6_prior_whole"),
        F.round((F.col("row_num") * 100), 2).alias("row_num_scaled"),
    )
)

result = (
    base_accounts
    .groupBy("account_type")
    .pivot("status", ["CLOSED", "FROZEN"])
    .agg(F.sum("row_num_scaled"))
)
