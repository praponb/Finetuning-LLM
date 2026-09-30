from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .filter(F.year("opened_date") >= 2025)
    .select("id", "customer_id", "balance", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .orderBy("id")
)
