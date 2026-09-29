from pyspark.sql import functions as F

base = df_accounts.filter(F.col("opened_date") <= "2024-11-01")

customer_id_summary = (
    base
    .groupBy("customer_id", "status")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    customer_id_summary
    .select("customer_id", "status", "status_list")
    .orderBy(F.col("customer_id").desc(), "status")
)
