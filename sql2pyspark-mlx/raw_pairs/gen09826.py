from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id")

ranked = (
    df_accounts
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "opened_date", "status")
)

result = (
    ranked
    .groupBy(F.date_trunc("WEEK", "opened_date").alias("week_start"), "status")
    .agg(
        F.count(F.when(F.col("status") == "FROZEN", 1)).alias("frozen_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
