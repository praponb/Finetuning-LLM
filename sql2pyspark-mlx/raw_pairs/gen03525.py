from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.balance").desc(), F.col("c.id").desc())
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.opened_date").desc(), F.col("c.id").desc())
w_part = Window.orderBy("account_balance", "id")
w4 = Window.partitionBy("account_account_type")
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("country", "account_account_type").orderBy(F.col("account_balance").desc(), "id")
w7 = Window.partitionBy("account_account_type").orderBy("account_balance", "id")
w8 = Window.partitionBy("account_account_type").orderBy("account_account_type_max_account_balance", "id")
w_ordered = Window.partitionBy("account_account_type").orderBy("prev_account_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("account_account_type").orderBy("account_account_type_max_account_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

kept = (
    df_customers
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.customer_id") == F.col("s.id")) & (F.col("ch.order_date") < F.col("s.signup_date")), "left_semi")
)

with_latest = (
    kept
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.name", "s.country", "s.city", "s.signup_date", F.col("c.status"))
)

base = (
    with_latest
    .filter(F.col("city").isNotNull() & F.col("status").isin(["OPEN", "CLOSED", "FROZEN"]))
    .withColumn("country_upper", F.upper("country"))
    .withColumn("status_prefix", F.substring("status", 1, 2))
    .withColumn("resolved_city", F.coalesce("city", F.lit("N/A")))
)

with_latest_customers = (
    base
    .alias("s")
    .join(df_accounts.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.opened_date") <= F.col("s.signup_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.name",
        "s.country",
        "s.resolved_city",
        F.col("c.balance").alias("account_balance"),
        F.col("c.account_type").alias("account_account_type"),
    )
)

with_windows = (
    with_latest_customers
    .select(
        "id",
        "account_account_type",
        F.ntile(10).over(w_part).alias("account_balance_bucket"),
        F.max("account_balance").over(w4).alias("account_account_type_max_account_balance"),
        F.count("account_balance").over(w5).alias("country_count_account_balance"),
        F.row_number().over(w6).alias("row_seq"),
        F.lag("account_balance", 1).over(w7).alias("prev_account_balance"),
    )
)

with_windows_customers = (
    with_windows
    .select(
        "*",
        F.lead("account_account_type_max_account_balance").over(w8).alias("next_account_account_type_max_account_balance"),
        F.lag("account_balance_bucket").over(w8).alias("prev_account_balance_bucket"),
        F.lead("account_balance_bucket").over(w8).alias("next_account_balance_bucket"),
        F.row_number().over(w8).alias("event_seq"),
    )
)

enriched = (
    with_windows_customers
    .filter(F.col("country_count_account_balance") <= 13)
    .select(
        "*",
        F.min("prev_account_balance").over(w_ordered).alias("running_min_prev_account_balance"),
        F.max("prev_account_balance").over(w_ordered).alias("running_max_prev_account_balance"),
        F.avg("prev_account_balance").over(w_ordered).alias("running_avg_prev_account_balance"),
        F.sum("prev_account_balance").over(w_ordered).alias("running_sum_prev_account_balance"),
        F.sum("country_count_account_balance").over(w10).alias("cumulative_country_count_account_balance"),
    )
)

result = (
    enriched
    .groupBy("account_account_type")
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
    )
    .filter(F.col("num_rows") >= 1)
    .orderBy(F.col("num_rows").desc())
)
