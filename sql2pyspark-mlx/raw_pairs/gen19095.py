from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_type").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy(F.col("last4_avg_account_type_length").asc_nulls_last(), "id")

valid = (
    df_accounts
    .filter(F.col("customer_id").isNotNull() & (F.col("balance") > 19000))
    .select(
        "*",
        F.length("account_type").alias("account_type_length"),
        F.coalesce("status", F.lit("NONE")).alias("status_clean"),
    )
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "account_type", "opened_date", "account_type_length")
)

scored = (
    ranked
    .filter(F.col("opened_date") <= "2024-06-01")
    .withColumn("last4_avg_account_type_length", F.avg("account_type_length").over(w2))
    .withColumn("last4_max_account_type_length", F.max("account_type_length").over(w2))
    .withColumn("last4_sum_account_type_length", F.sum("account_type_length").over(w2))
    .withColumn("last4_count_account_type_length", F.count("account_type_length").over(w2))
)

deduped = (
    scored
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(
        F.when(F.col("last4_sum_account_type_length") >= 420, "PREMIUM").otherwise("STANDARD").alias("last4_sum_account_type_length_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
        F.count_if(F.col("account_type") == "SAVINGS").alias("savings_events"),
    )
)
