from pyspark.sql import functions as F

src = df_accounts.filter(F.col("balance") >= 38000)

summary = (
    src
    .groupBy("customer_id")
    .agg(F.sort_array(F.collect_set("account_type")).alias("account_type_list"))
)

result = (
    summary
    .select("customer_id", "account_type_list")
    .orderBy(F.col("customer_id").desc())
)
