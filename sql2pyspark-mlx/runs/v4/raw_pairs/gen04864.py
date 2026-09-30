from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("balance").desc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("opened_date").desc_nulls_last(), "id")

valid = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2024-04-05").cast("date")) & (F.col("balance") >= 50000))
    .select(
        "id",
        "balance",
        "opened_date",
        "status",
        F.concat_ws("-", "status", "account_type").alias("status_account_type_key"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_filled"),
    )
)

latest = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.is_flagged")), "left_semi")
)

filtered = (
    eligible
    .filter(F.col("opened_date") > F.lit("2024-04-16").cast("date"))
    .select(
        "id",
        "opened_date",
        "status",
        "status_account_type_key",
        "status_filled",
        "rank_in_group",
        F.last_day("opened_date").alias("opened_date_month_end"),
        F.add_months("opened_date", 3).alias("opened_date_plus_months"),
    )
)

deduped = (
    filtered
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "opened_date",
        "status",
        "status_account_type_key",
        "status_filled",
        "rank_in_group",
        "opened_date_month_end",
        "rn",
        F.when(F.col("status").rlike("^(OPEN|FROZEN)$"), "GROUP_B").otherwise("GROUP_A").alias("status_zone"),
    )
)
