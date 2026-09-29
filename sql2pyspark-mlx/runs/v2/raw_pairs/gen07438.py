from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("opened_date") < "2023-09-14")
    .groupBy("status")
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
    .orderBy(F.col("status").desc())
)
