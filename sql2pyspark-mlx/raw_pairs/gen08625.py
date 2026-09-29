from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "customer_id").orderBy(F.col("opened_date").desc_nulls_last())

latest = (
    df_accounts
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "customer_id", "opened_date")
)

result = (
    latest
    .withColumn("week_start", F.date_trunc("WEEK", "opened_date"))
    .rollup("week_start")
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
        F.countDistinct("customer_id").alias("distinct_customer_ids_2"),
        F.grouping("week_start").alias("gid"),
    )
    .orderBy(F.col("distinct_customer_ids_2").desc_nulls_last())
)
