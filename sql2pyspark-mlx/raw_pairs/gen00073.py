from pyspark.sql import functions as F

by_dimension = (
    df_accounts
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "left")
    .groupingSets(
        [["country", "status"], ["country"], []],
        F.col("d.country").alias("country"),
        F.col("e.status").alias("status"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("e.balance")), 2).alias("avg_balance"),
        F.grouping_id().alias("level_id"),
    )
)

result = (
    by_dimension
    .select(
        "status",
        "num_rows",
        "level_id",
        F.when(F.col("status").rlike("^(FROZEN|CLOSED)$"), "GROUP_A").when(F.col("status").rlike("^(OPEN)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("status").asc_nulls_first())
)
