from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(((F.col("customer_id") == 19) & F.col("account_type").isin(["CHECKING", "SAVINGS", "CREDIT"])) | F.col("status").isin(["OPEN", "FROZEN", "CLOSED"]))
    .groupBy("status", F.month("opened_date").alias("opened_month"))
    .agg(F.sort_array(F.collect_set("account_type")).alias("account_type_list"))
)
