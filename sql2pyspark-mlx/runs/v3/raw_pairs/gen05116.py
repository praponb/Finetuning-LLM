from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .groupBy(
        F.when(F.col("merchant").isin(["Amazon", "Lazada"]), "group_a").otherwise("other").alias("merchant_group"),
    )
    .agg(F.sort_array(F.collect_set("txn_type")).alias("txn_type_list"))
    .orderBy("merchant_group")
)
