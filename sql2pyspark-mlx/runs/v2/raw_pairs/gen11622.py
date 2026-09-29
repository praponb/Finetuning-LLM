from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("started_at").desc(), "id")

result = (
    df_sessions
    .filter(F.col("customer_id") == 7)
    .select("id", "started_at", F.row_number().over(w).alias("rn"))
)
