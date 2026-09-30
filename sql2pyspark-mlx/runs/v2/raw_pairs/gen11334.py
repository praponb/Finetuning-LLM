from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("balance", "id")

result = (
    df_accounts
    .filter(F.year("opened_date") == 2024)
    .select(
        "id",
        "customer_id",
        "opened_date",
        "balance",
        F.lead("balance", 1).over(w).alias("next_balance"),
        F.row_number().over(w2).alias("row_num"),
    )
)
