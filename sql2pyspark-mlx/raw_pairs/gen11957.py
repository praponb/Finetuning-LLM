from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("account_id", "merchant").orderBy("amount", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("amount", "id")

result = (
    df_transactions
    .select(
        "id",
        "is_flagged",
        F.avg("amount").over(w_recent).alias("last3_avg_amount"),
        F.min("amount").over(w_recent).alias("last3_min_amount"),
        F.count("amount").over(w_recent).alias("last3_count_amount"),
        F.lead("amount", 2).over(w2).alias("next_amount"),
    )
)
