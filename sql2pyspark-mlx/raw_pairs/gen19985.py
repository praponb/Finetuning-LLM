from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("opened_date")

result = (
    df_stores
    .filter(F.col("opened_date") < "2025-03-01")
    .select(
        "id",
        "city",
        "opened_date",
        F.percent_rank().over(w).alias("opened_date_pct_rank"),
    )
)
