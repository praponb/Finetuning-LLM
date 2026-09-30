from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("duration_sec", "id")
w2 = Window.orderBy("duration_sec")

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 257)
    .select(
        "id",
        "duration_sec",
        F.row_number().over(w).alias("row_num"),
        F.percent_rank().over(w2).alias("duration_sec_pct_rank"),
    )
)
