from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id").orderBy("balance", "id")
w2 = Window.partitionBy("customer_id", "account_type").orderBy(F.col("balance").desc(), "id")
w3 = Window.orderBy("balance", "id")
w_ordered = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("status")
w6 = Window.partitionBy("status", "account_type").orderBy(F.col("next_balance").desc_nulls_last(), "id")

base = (
    df_accounts
    .filter(F.col("opened_date") >= "2024-12-18")
    .withColumn("status_account_type_key", F.concat_ws("-", "status", "account_type"))
    .withColumn("status_clean", F.coalesce("status", F.lit("DIRECT")))
)

enriched = (
    base
    .select(
        "*",
        F.row_number().over(w_recent).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
        F.lead("balance", 1, 0).over(w3).alias("next_balance"),
        F.last("status", ignorenulls=True).over(w_ordered).alias("last_known_status"),
        F.avg("balance").over(w5).alias("status_avg_balance"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "inner")
    .filter(F.col("p.age") >= 63)
    .select("src.*", F.col("p.segment"))
)

eligible = (
    with_dims
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

latest = (
    eligible
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select("id", "segment", "balance", "next_balance")
    .unpivot(
        ids=["id", "segment"],
        values=["balance", "next_balance"],
        variableColumnName="measure",
        valueColumnName="value",
    )
)
