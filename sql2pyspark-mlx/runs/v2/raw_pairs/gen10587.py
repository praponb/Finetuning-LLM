from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("txn_time").desc())

result = (
    df_transactions
    .filter(F.to_date("txn_time") >= "2023-03-01")
    .select("id", "txn_time", F.rank().over(w).alias("rnk"))
    .orderBy(F.col("rnk").desc())
)
