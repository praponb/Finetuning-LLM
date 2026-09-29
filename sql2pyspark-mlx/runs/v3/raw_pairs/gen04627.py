from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("amount")

result = (
    df_payments
    .filter(F.col("amount") < 1590)
    .select("id", "amount", F.rank().over(w).alias("amount_rank"))
)
