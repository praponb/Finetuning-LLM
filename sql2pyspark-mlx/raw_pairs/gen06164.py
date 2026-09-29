from pyspark.sql import functions as F

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-06-02"))) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.round("balance", 2).alias("balance_rounded"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("status_filled"),
    )
)

result = (
    cleaned
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "left")
    .groupingSets(
        [["segment", "country"], ["segment"], []],
        F.col("c.segment").alias("segment"),
        F.col("c.country").alias("country"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.balance")), 2).alias("total_balance"),
        F.avg(F.col("a.balance_rounded")).alias("avg_balance_rounded"),
    )
)
