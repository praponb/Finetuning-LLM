from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time").desc_nulls_last(), "id")

latest = (
    df_transactions
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "account_id", "txn_time", "merchant")
)

result = (
    latest
    .withColumn("activity_month", F.date_trunc("MONTH", "txn_time"))
    .groupingSets(
        [["merchant", "activity_month"], ["merchant"]],
        "merchant",
        "activity_month",
    )
    .agg(
        F.count(F.when(F.col("merchant") == "Starbucks", 1)).alias("starbucks_count"),
        F.count(F.when(F.col("merchant") == "Lazada", 1)).alias("lazada_count"),
        F.max("txn_time").alias("last_seen"),
        F.sort_array(F.collect_set("merchant")).alias("merchant_list"),
        F.count(F.when(F.col("merchant") == "Grab", 1)).alias("grab_count"),
    )
    .orderBy(F.col("merchant").desc())
)
