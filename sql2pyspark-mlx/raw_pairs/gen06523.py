from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_windows = (
    df_transactions
    .select(
        "id",
        "amount",
        "txn_time",
        "merchant",
        F.count("amount").over(w_running).alias("running_count_amount"),
        F.max("amount").over(w_running).alias("running_max_amount"),
        F.min("amount").over(w_running).alias("running_min_amount"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "amount",
        "txn_time",
        "running_count_amount",
        "running_max_amount",
        "running_min_amount",
    )
)
