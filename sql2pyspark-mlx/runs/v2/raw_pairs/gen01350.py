from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at")

result = (
    df_sessions
    .filter(F.col("browser") == "Safari")
    .select(
        "id",
        "customer_id",
        "started_at",
        F.percent_rank().over(w).alias("started_at_pct_rank"),
    )
)
