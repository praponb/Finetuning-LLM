from pyspark.sql import functions as F

base = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-02-28"))) & ~F.col("status").isin(["OPEN", "FROZEN"]))
    .select(
        "id",
        "customer_id",
        "balance",
        "opened_date",
        "status",
        F.round("balance", 2).alias("balance_rounded"),
    )
)

result = (
    base
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["city", "status"], ["city"]],
        F.col("d.city").alias("city"),
        F.col("a.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.round(F.sum(F.col("a.balance_rounded")), 2).alias("total_balance_rounded"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy("total_balance_rounded")
)
