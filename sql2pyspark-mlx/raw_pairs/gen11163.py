from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("age", "id")
w2 = Window.partitionBy("account_type").orderBy(F.col("age").asc_nulls_first(), "id")

base = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-08-20").cast("date"))
    .withColumn("balance_band", F.when(F.col("balance") >= 75000, "High").when(F.col("balance") >= 37000, "Medium").otherwise("Low"))
    .withColumn("account_type_group", F.when(F.col("account_type").isin(["CHECKING", "CREDIT"]), "Tier 1").otherwise("Other"))
    .withColumn("status_clean", F.coalesce("status", F.lit("NONE")))
)

not_flagged = (
    base
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") < F.col("s.opened_date")), "left_anti")
)

joined = (
    not_flagged
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select("e.*", F.col("c.segment"), F.col("c.age"), F.col("c.credit_limit"))
)

scored = (
    joined
    .select(
        "*",
        F.lag("credit_limit").over(w).alias("prev_credit_limit"),
        F.lead("credit_limit").over(w).alias("next_credit_limit"),
        F.lead("balance").over(w).alias("next_balance"),
        F.lag("balance").over(w).alias("prev_balance"),
        F.row_number().over(w).alias("row_seq"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    deduped
    .select("status", "id")
    .subtract(
        df_accounts
        .filter(F.col("balance") < 31000)
        .select("status", "id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.status") == F.col("k.status")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .rollup("quarter_start", "customer_id")
    .agg(
        F.max(F.when(F.col("status") == "FROZEN", F.col("balance"))).alias("frozen_max_balance"),
        F.countDistinct("segment").alias("distinct_segments"),
        F.sum(F.when(F.col("segment") == "Corporate", F.col("next_balance")).otherwise(0)).alias("corporate_next_balance"),
        F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("distinct_segments").asc_nulls_first())
)
