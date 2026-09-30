from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("account_id").orderBy("amount", "id")
w_ordered = Window.partitionBy("txn_type").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .select(
        "*",
        F.lag("amount").over(w_running).alias("prev_amount"),
        F.lead("amount").over(w_running).alias("next_amount"),
        F.row_number().over(w_running).alias("event_seq"),
        F.ntile(10).over(w_ordered).alias("amount_bucket"),
    )
)
