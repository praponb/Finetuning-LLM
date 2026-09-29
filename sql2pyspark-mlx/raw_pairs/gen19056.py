from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("amount").desc(), "id")
w_ordered = Window.partitionBy("txn_type", "merchant")
w3 = Window.orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)

result = (
    df_transactions
    .select(
        "id",
        "amount",
        "is_flagged",
        F.ntile(4).over(w).alias("amount_bucket"),
        F.count("amount").over(w_ordered).alias("txn_type_count_amount"),
        F.count("amount").over(w3).alias("amount_count_90d"),
    )
)
