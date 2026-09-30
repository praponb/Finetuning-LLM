from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")

result = (
    df_accounts
    .filter(F.col("balance") >= 74000)
    .select("id", "customer_id", "opened_date", F.row_number().over(w).alias("rn"))
)
