from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("account_type").isin(["CHECKING", "LOAN"]))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "account_type")
    .agg(
        F.sum(F.when(F.col("status") == "CLOSED", 1).otherwise(0)).alias("closed_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("account_type"))).alias("account_type_values"),
    )
    .filter(F.col("closed_count") > 72)
    .select("opened_month", "account_type", "closed_count", "account_type_values")
    .orderBy(F.col("closed_count").desc(), "opened_month", "account_type")
    .limit(5)
)
