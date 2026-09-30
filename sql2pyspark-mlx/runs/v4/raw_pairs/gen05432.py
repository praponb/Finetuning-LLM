from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("balance").asc_nulls_first())
w2 = Window.partitionBy("account_type").orderBy(F.col("balance").desc(), "id")

scored = (
    df_accounts
    .withColumn("balance_cume_dist", F.cume_dist().over(w))
    .withColumn("seq_no", F.row_number().over(w2))
)

result = (
    scored
    .groupBy(
        "status",
        F.when(F.col("balance_cume_dist") >= 0.62, "PREMIUM").otherwise("STANDARD").alias("balance_cume_dist_tier"),
    )
    .agg(
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.countDistinct("status").alias("distinct_status"),
        F.max("balance_cume_dist").alias("max_balance_cume_dist"),
    )
    .orderBy(F.col("distinct_status").desc_nulls_first())
)
