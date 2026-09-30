from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("days_since_opened_date").desc())

filtered = (
    df_accounts
    .filter(~F.col("status").eqNullSafe(F.lit("CLOSED")))
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
)

valid = (
    filtered
    .filter(F.col("balance") < 57000)
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        "days_since_opened_date",
        F.concat("account_type", F.lit(" - "), "status").alias("account_type_status"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "customer_id", "account_type", "status", "account_type_status")
)

kept = (
    ranked
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.amount") < 2100), "left_anti")
)

result = (
    kept
    .groupBy("status")
    .agg(
        F.count_if(F.col("status") == "CLOSED").alias("closed_events"),
        F.count(F.when(F.col("account_type") == "LOAN", 1)).alias("loan_count"),
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
    )
    .filter(F.col("loan_count") >= 1)
    .orderBy(F.col("loan_count").desc())
)
