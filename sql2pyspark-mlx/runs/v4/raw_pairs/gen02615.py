from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "merchant").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy(F.col("amount").desc())
w_grp = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)

result = (
    df_transactions
    .withColumn("last_known_merchant", F.last("merchant", ignorenulls=True).over(w))
    .withColumn("amount_cume_dist", F.cume_dist().over(w2))
    .withColumn("amount_max_7d", F.max("amount").over(w_grp))
)
