from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .select("id", "customer_id", "balance", F.row_number().over(w).alias("rn"))
    .filter(F.col("rn") <= 3)
    .select("id", "customer_id", "balance")
)
