from pyspark.sql import functions as F

result = (
    df_accounts
    .filter((F.col("account_type") != "CREDIT") & (F.col("balance").between(28000, 55000) | (F.col("opened_date") <= "2023-05-01")))
    .groupBy(
        F.when(F.col("status").isin(["CLOSED", "FROZEN"]), "priority").otherwise("other").alias("status_group"),
    )
    .agg(
        F.round(F.avg("balance"), 2).alias("avg_balance"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
        F.countDistinct("balance").alias("unique_statuss"),
    )
    .orderBy(F.col("avg_balance").desc())
)
