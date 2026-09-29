from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc())
w2 = Window.partitionBy("city").orderBy("opened_date")

result = (
    df_stores
    .select(
        "id",
        "sqft",
        "city",
        "opened_date",
        F.dense_rank().over(w).alias("dense_rnk"),
        F.percent_rank().over(w2).alias("opened_date_pct_rank"),
    )
)
