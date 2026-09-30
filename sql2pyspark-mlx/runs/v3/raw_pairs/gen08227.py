from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("started_at")
w2 = Window.partitionBy("country").orderBy("started_at", "id")

result = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .select(
        "id",
        "customer_id",
        "started_at",
        "country",
        F.percent_rank().over(w).alias("started_at_pct_rank"),
        F.row_number().over(w2).alias("rn"),
    )
)
