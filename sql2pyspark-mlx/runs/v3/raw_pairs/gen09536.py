from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc())
w2 = Window.partitionBy("carrier").orderBy("cost")

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([3, 4, 10]))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.rank().over(w).alias("shipped_date_rank"),
        F.rank().over(w2).alias("rnk"),
    )
)
