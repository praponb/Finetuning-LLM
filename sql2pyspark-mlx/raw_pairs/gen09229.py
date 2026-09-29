from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w2 = Window.partitionBy(F.month("opened_date")).orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)

result = (
    df_accounts
    .select(
        "id",
        "account_type",
        F.row_number().over(w).alias("seq_no"),
        F.avg("balance").over(w2).alias("balance_avg_6"),
    )
)
