from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date")

result = (
    df_shipments
    .select(
        "id",
        "carrier",
        "shipped_date",
        F.rank().over(w).alias("shipped_date_rank"),
    )
    .orderBy(F.col("carrier").desc())
)
