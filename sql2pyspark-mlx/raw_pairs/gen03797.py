from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("customer_id").orderBy(F.col("balance").desc_nulls_last())
w2 = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-6, -1)

result = (
    df_accounts
    .select(
        "*",
        F.rank().over(w_running).alias("balance_rank"),
        F.avg("balance").over(w2).alias("balance_avg_6_prior"),
    )
)
