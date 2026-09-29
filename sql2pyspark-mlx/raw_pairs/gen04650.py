from pyspark.sql import functions as F

result = (
    df_payments
    .filter(~((F.col("status") != "REFUNDED") & (F.col("status") == "REFUNDED")))
    .groupBy("method")
    .agg(F.sort_array(F.collect_set("method")).alias("method_list"))
    .orderBy("method")
)
