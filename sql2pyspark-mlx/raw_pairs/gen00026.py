from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status", "customer_id").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("balance").asc_nulls_first())

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        F.max("balance").over(w_ordered).alias("running_max_balance"),
        F.avg("balance").over(w_ordered).alias("running_avg_balance"),
        F.count("balance").over(w_ordered).alias("running_count_balance"),
        F.rank().over(w2).alias("balance_rank"),
    )
)
