from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("balance", "id")
w2 = Window.partitionBy(F.month("opened_date"), "customer_id")
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("customer_id").orderBy("seq_no_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_accounts
    .filter(F.col("opened_date") >= F.lit("2023-06-15").cast("date"))
    .select(
        "*",
        F.when(F.col("balance") >= 88000, "High").when(F.col("balance") >= 21000, "Medium").otherwise("Low").alias("balance_level"),
        F.when(F.col("status") == "FROZEN", 1).otherwise(0).alias("is_frozen"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "customer_id",
        "balance_level",
        F.row_number().over(w).alias("seq_no"),
        F.count("is_frozen").over(w2).alias("opened_date_month_count_is_frozen"),
    )
)

base = (
    with_windows
    .filter(F.col("customer_id").isNotNull() & (F.col("seq_no") <= 4))
    .select(
        "*",
        F.greatest("seq_no", "opened_date_month_count_is_frozen").alias("greatest_seq_no_opened_date_month_count_is_frozen"),
        F.coalesce("seq_no", F.lit(0)).alias("seq_no_filled"),
    )
)

eligible = (
    base
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id").isin([5, 31, 36])), "left_anti")
)

scored = (
    eligible
    .filter(F.col("seq_no") < 8)
    .select(
        "id",
        "customer_id",
        "seq_no",
        "opened_date_month_count_is_frozen",
        "greatest_seq_no_opened_date_month_count_is_frozen",
        F.stddev_samp("opened_date_month_count_is_frozen").over(w3).alias("customer_id_stddev_samp_opened_date_month_count_is_frozen"),
        F.sum("greatest_seq_no_opened_date_month_count_is_frozen").over(w4).alias("cumulative_greatest_seq_no_opened_date_month_count_is_frozen"),
    )
)

enriched = (
    scored
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.opened_date_month_count_is_frozen",
        "e.customer_id_stddev_samp_opened_date_month_count_is_frozen",
        "e.cumulative_greatest_seq_no_opened_date_month_count_is_frozen",
        F.col("d.segment"),
    )
)

enriched_accounts = (
    enriched
    .alias("a")
    .join(df_customers.alias("b"), F.col("a.customer_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.country"))
)

result = (
    enriched_accounts
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupingSets(
        [["customer_country", "account_segment"], ["customer_country"], []],
        F.col("c.country").alias("customer_country"),
        F.col("a.segment").alias("account_segment"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.cumulative_greatest_seq_no_opened_date_month_count_is_frozen")).alias("min_cumulative_greatest_seq_no_opened_date_month_count_is_frozen"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("rows_total").asc_nulls_first())
)
