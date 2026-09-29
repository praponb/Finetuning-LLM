from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("merchant").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "account_id",
        F.max("amount").over(w_hist).alias("last7_max_amount"),
        F.min("amount").over(w_hist).alias("last7_min_amount"),
        F.sum("amount").over(w_hist).alias("last7_sum_amount"),
        F.avg("amount").over(w_hist).alias("last7_avg_amount"),
    )
)
