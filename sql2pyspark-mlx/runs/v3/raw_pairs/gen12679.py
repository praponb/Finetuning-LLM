from pyspark.sql import functions as F

result = (
    df_transactions
    .groupBy(
        F.when(F.col("txn_type").isin(["CREDIT", "DEBIT"]), "group_a").otherwise("other").alias("txn_type_group"),
    )
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
    )
    .filter(F.col("amount_range") < 43)
    .select("txn_type_group", "amount_range", "merchant_list")
    .orderBy(F.col("amount_range").desc())
)
