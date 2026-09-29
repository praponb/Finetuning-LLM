from pyspark.sql import functions as F

dim_totals = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("p"), F.col("a.customer_id") == F.col("p.id"), "inner")
    .rollup(F.col("p.country").alias("country"), F.col("a.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg(F.col("a.balance")), 2).alias("avg_balance"),
    )
)

result = (
    dim_totals
    .select(
        "status",
        "cnt",
        F.when(F.col("status").rlike("^(CLOSED|FROZEN)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("status_zone"),
    )
    .orderBy(F.col("cnt").desc_nulls_first())
)
