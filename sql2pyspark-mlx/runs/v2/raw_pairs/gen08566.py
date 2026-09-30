from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id", "status").orderBy("opened_date")

result = (
    df_accounts
    .select(
        "id",
        "customer_id",
        "status",
        "opened_date",
        F.lead("opened_date", 1).over(w).alias("next_opened_date"),
        F.dense_rank().over(w2).alias("dense_rnk"),
    )
)
