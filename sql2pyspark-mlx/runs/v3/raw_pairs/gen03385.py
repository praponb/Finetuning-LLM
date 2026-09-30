from pyspark.sql import functions as F

src = (
    df_accounts
    .filter((F.col("balance") < 32000) & F.col("status").isin(["FROZEN", "OPEN"]))
)

summary = (
    src
    .groupBy("status", "account_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("status"))).alias("status_values"),
        F.countDistinct("balance").alias("unique_account_types"),
    )
)

result = summary.orderBy(F.col("unique_account_types").desc())
