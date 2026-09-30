from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("balance").desc(), "id")

result = (
    df_accounts
    .select("id", "balance", F.row_number().over(w).alias("rn"))
    .orderBy(F.col("rn").desc())
)
