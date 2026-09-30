from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-2, Window.currentRow)

cleaned = (
    df_accounts
    .filter(F.col("balance") > 59000)
    .select(
        "id",
        "customer_id",
        "balance",
        "status",
        F.initcap(F.trim("status")).alias("status_title"),
    )
)

result = (
    cleaned
    .filter(F.col("customer_id").isNotNull())
    .withColumn("last3_min_balance", F.min("balance").over(w))
    .withColumn("last3_max_balance", F.max("balance").over(w))
    .withColumn("last3_sum_balance", F.sum("balance").over(w))
)
