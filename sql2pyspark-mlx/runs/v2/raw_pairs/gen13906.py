from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("shipped_date").desc())

result = (
    df_shipments
    .select(
        "id",
        "shipped_date",
        F.percent_rank().over(w).alias("shipped_date_pct_rank"),
    )
)
