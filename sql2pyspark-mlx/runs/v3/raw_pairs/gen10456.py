from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"))
    .orderBy("merchant")
)
