from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("amount").desc(), "id")

result = (
    df_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "merchant",
        "amount",
        "account_id",
        F.row_number().over(w).alias("rnk"),
    )
    .filter(F.col("rnk") <= 2)
    .select("id", "merchant", "amount", "account_id")
)
