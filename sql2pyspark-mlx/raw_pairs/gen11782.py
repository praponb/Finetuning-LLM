from pyspark.sql import functions as F

by_dimension = (
    df_accounts
    .alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupingSets(
        [["country", "status"], ["country"]],
        F.col("c.country").alias("country"),
        F.col("s.status").alias("status"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("s.balance")).alias("total_balance"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    by_dimension
    .select(
        "status",
        "rows_total",
        "total_balance",
        "level_id",
        F.when(F.col("status").rlike("^(CLOSED|FROZEN)$"), "CORE").otherwise("PRIORITY").alias("status_zone"),
    )
)
