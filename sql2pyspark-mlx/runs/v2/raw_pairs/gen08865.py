from pyspark.sql import functions as F

src = df_accounts.filter(F.col("customer_id") == 30)

agg = (
    src
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = (
    agg
    .select("status", "status_list")
    .orderBy(F.col("status").desc())
)
