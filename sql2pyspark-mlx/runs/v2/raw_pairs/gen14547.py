from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        F.percent_rank().over(w).alias("start_date_pct_rank"),
    )
    .orderBy(F.col("start_date_pct_rank").desc())
)
