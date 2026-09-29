from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy("merchant")
    .agg(F.sort_array(F.collect_set("merchant")).alias("merchant_list"))
    .orderBy("merchant")
)
