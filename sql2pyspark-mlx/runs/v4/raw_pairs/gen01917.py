from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy("balance", "id")
w2 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("balance").asc_nulls_first())
w4 = Window.partitionBy("account_type").orderBy("balance", "id")
w5 = Window.partitionBy("account_type", "status").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_accounts
    .withColumn("prev_balance", F.lag("balance", 2).over(w))
    .withColumn("cumulative_balance", F.sum("balance").over(w2))
    .withColumn("balance_cume_dist", F.cume_dist().over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("last_known_status", F.last("status", ignorenulls=True).over(w5))
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= F.col("s.opened_date")), "left_anti")
)

joined = (
    not_flagged
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.city"), F.col("b.name"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select(
        "s.*",
        F.col("d.name").alias("customer_name"),
        F.col("d.credit_limit"),
        F.col("d.country"),
    )
)

kept_keys = (
    with_dims
    .select("id", "account_type")
    .subtract(
        df_accounts
        .filter(F.col("balance") >= 19000)
        .select("id", "account_type")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

joined_accounts = (
    reconciled
    .alias("src")
    .join(df_customers.alias("p"), F.col("src.customer_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.name").alias("customer_name_2"))
)

cleaned = (
    joined_accounts
    .filter(F.col("country").isNotNull() & (F.col("prev_balance") >= 39000))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "prev_balance",
        "cumulative_balance",
        "balance_cume_dist",
        "row_seq",
        "city",
        "name",
        "customer_name",
        "credit_limit",
        "country",
        F.substring("name", 1, 4).alias("name_prefix"),
        F.round((F.col("balance_cume_dist") * 1.07), 2).alias("balance_cume_dist_scaled"),
    )
)

totals = (
    cleaned
    .withColumn("balance_cume_dist_scaled_tier", F.when(F.col("balance_cume_dist_scaled") >= 75, "HIGH").otherwise("LOW"))
    .withColumn("prev_balance_tier", F.when(F.col("prev_balance") >= 88000, "HIGH").otherwise("LOW"))
    .rollup("balance_cume_dist_scaled_tier", "prev_balance_tier", "city")
    .agg(
        F.max("balance_cume_dist_scaled").alias("max_balance_cume_dist_scaled"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("prev_balance")).otherwise(0)).alias("savings_prev_balance"),
        F.round(F.sum("prev_balance") / F.nullif(F.count(F.when(F.col("city") == "Bangkok", 1)), F.lit(0)), 2).alias("prev_balance_per_bangkok"),
        F.grouping("balance_cume_dist_scaled_tier").alias("grouping_level"),
    )
)

result = (
    totals
    .select(
        "balance_cume_dist_scaled_tier",
        "prev_balance_tier",
        "city",
        "max_balance_cume_dist_scaled",
        "savings_prev_balance",
    )
)
