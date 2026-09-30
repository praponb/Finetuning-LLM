from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .select(
        "id",
        "status",
        F.lead("balance").over(w).alias("next_balance"),
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("event_seq"),
    )
)
