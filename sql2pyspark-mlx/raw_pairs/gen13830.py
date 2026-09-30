from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.to_date("txn_time") < "2023-08-31")
    .select(
        "id",
        "merchant",
        "amount",
        "txn_type",
        F.row_number().over(w).alias("row_num"),
    )
    .filter(F.col("row_num") <= 2)
)
