from pyspark.sql import functions as F

valid = (
    df_accounts
    .filter((F.col("opened_date") >= F.to_date(F.lit("2023-04-23"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.floor("balance").alias("balance_whole"),
        F.abs("balance").alias("abs_balance"),
    )
)

result = (
    valid
    .alias("e")
    .join(df_customers.alias("d"), F.col("e.customer_id") == F.col("d.id"), "inner")
    .rollup(F.col("d.city").alias("city"), F.col("d.segment").alias("segment"))
    .agg(
        F.count("*").alias("cnt"),
        F.sum(F.col("e.balance")).alias("total_balance"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("total_balance").desc_nulls_last())
)
