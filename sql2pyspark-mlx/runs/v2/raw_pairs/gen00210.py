from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type")

result = (
    df_transactions
    .filter(~F.col("is_flagged"))
    .select(
        "id",
        "txn_type",
        "amount",
        F.max("amount").over(w).alias("txn_type_max_amount"),
    )
    .orderBy("txn_type_max_amount")
)
